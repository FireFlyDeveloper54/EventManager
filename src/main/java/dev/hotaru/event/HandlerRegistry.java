package dev.hotaru.event;

import dev.hotaru.event.annotations.EventTarget;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.ArrayList;
import java.lang.ref.WeakReference;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.Executors;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Owns handler registration state for an {@link EventManager}: the handler
 * table, the dispatch cache, weak-handler bookkeeping, and all
 * register/unregister/query/binding logic.
 *
 * <p>Package-private: the public API surface stays on {@link EventManager},
 * which delegates here. The registry calls back into its owning bus for
 * lifecycle guards ({@code owner.ensureOpen()}) and sticky replay
 * ({@code owner.replaySticky(...)}).
 */
final class HandlerRegistry {

    private static final java.util.logging.Logger log = java.util.logging.Logger.getLogger(HandlerRegistry.class.getName());

    final EventManager owner;

    HandlerRegistry(EventManager owner) {
        this.owner = owner;
    }

    private final ConcurrentMap<Class<? extends Event>, Handler[]> eventHandlers =
            new ConcurrentHashMap<Class<? extends Event>, Handler[]>();
    private final ConcurrentMap<Class<?>, CachedDispatch> dispatchCache =
            new ConcurrentHashMap<Class<?>, CachedDispatch>();
    // Weak keys: mutation stamps are bookkeeping and must not keep event
    // classes (or their class loaders) alive. Only independent get/put calls
    // are made against this map, so the synchronized wrapper is sufficient.
    private final Map<Class<?>, Long> typeMutationStamps =
            Collections.synchronizedMap(new WeakHashMap<Class<?>, Long>());

    private static final ClassValue<Class<? extends Event>[]> EVENT_HIERARCHIES =
            new ClassValue<Class<? extends Event>[]>() {
                @Override
                protected Class<? extends Event>[] computeValue(Class<?> type) {
                    LinkedHashSet<Class<? extends Event>> collected = new LinkedHashSet<Class<? extends Event>>();
                    collectEventHierarchy(type, collected);
                    @SuppressWarnings("unchecked")
                    Class<? extends Event>[] array = (Class<? extends Event>[]) collected.toArray(new Class<?>[collected.size()]);
                    return array;
                }
            };

    private final AtomicLong registrationOrder = new AtomicLong();
    private final AtomicLong mutationVersion = new AtomicLong();

    // Drives the amortized auto-purge of garbage-collected weak handlers:
    // set when a weak handler is registered, cleared by purgeDeadHandlers
    // when a scan finds no weak handlers left at all.
    private volatile boolean hasWeakHandlers = false;
    // Counts dispatches towards the next opportunistic weak-handler purge.
    private final AtomicLong dispatchesSinceWeakPurge = new AtomicLong();

    private final class HandlerSubscription implements Subscription {
        private final Handler handler;
        private final AtomicBoolean subscribed = new AtomicBoolean(true);

        private HandlerSubscription(Handler handler) {
            this.handler = handler;
        }

        @Override
        public void unsubscribe() {
            if (!subscribed.compareAndSet(true, false)) {
                return;
            }
            handler.active.set(false);
            removeHandlers(handler.eventType, new Predicate<Handler>() {
                @Override
                public boolean test(Handler candidate) {
                    return candidate == handler;
                }
            });
        }

        @Override
        public boolean isSubscribed() {
            if (!subscribed.get() || !handler.active.get()) {
                return false;
            }
            if (handler.weakListener != null && handler.weakListener.get() == null) {
                handler.active.set(false);
                return false;
            }
            Handler[] handlers = eventHandlers.get(handler.eventType);
            if (handlers == null) {
                return false;
            }
            for (int i = 0; i < handlers.length; i++) {
                if (handlers[i] == handler) {
                    return true;
                }
            }
            return false;
        }
    }

    private final class ListenerSubscription implements Subscription {
        private final Handler[] handlers;
        private final AtomicBoolean subscribed = new AtomicBoolean(true);

        private ListenerSubscription(Handler[] handlers) {
            this.handlers = handlers;
        }

        @Override
        public void unsubscribe() {
            if (!subscribed.compareAndSet(true, false)) {
                return;
            }
            final Set<Handler> owned = Collections.newSetFromMap(
                    new IdentityHashMap<Handler, Boolean>());
            Collections.addAll(owned, handlers);
            removeHandlers(new Predicate<Handler>() {
                @Override
                public boolean test(Handler candidate) {
                    return owned.contains(candidate);
                }
            });
        }

        @Override
        public boolean isSubscribed() {
            if (!subscribed.get()) {
                return false;
            }
            for (Handler handler : handlers) {
                if (!handler.active.get()) {
                    continue;
                }
                if (handler.weakListener != null && handler.weakListener.get() == null) {
                    handler.active.set(false);
                    continue;
                }
                Handler[] current = eventHandlers.get(handler.eventType);
                if (current != null) {
                    for (Handler candidate : current) {
                        if (candidate == handler) {
                            return true;
                        }
                    }
                }
            }
            return false;
        }
    }

    private static final class CachedDispatch {
        private final Handler[] handlers;

        private CachedDispatch(Handler[] handlers) {
            this.handlers = handlers;
        }
    }

    public void register(Object... listeners) {
        if (listeners == null) {
            return;
        }
        for (Object listener : listeners) {
            register(listener);
        }
    }

    /**
     * Registers an annotated listener and returns a subscription handle.
     * Registering the same listener instance again is a no-op: handler
     * equality deduplicates, and the returned handle is a NOOP subscription.
     *
     * @param listener the listener object (annotated methods/fields are bound)
     * @return a subscription handle; {@link Subscription#NOOP} when already registered
     */
    public Subscription register(Object listener) {
        return register(listener, (Class<? extends Event>) null);
    }

    /**
     * Registers a listener, optionally restricted to handlers bound for the
     * exact event class. Consumer listeners require a non-null event class and
     * may be registered multiple times (each call yields an independent
     * subscription); annotated listeners are deduplicated per instance.
     */
    public Subscription register(Object listener, Class<? extends Event> eventClass) {
        if (listener == null) {
            return Subscription.NOOP;
        }
        if (listener instanceof Consumer<?>) {
            if (eventClass == null) {
                return Subscription.NOOP;
            }
            @SuppressWarnings("unchecked")
            Consumer<Event> consumer = (Consumer<Event>) listener;
            @SuppressWarnings("unchecked")
            Class<Event> evt = (Class<Event>) eventClass;
            return register(evt, consumer);
        }
        if (listener instanceof Class<?>) {
            return register((Class<?>) listener, eventClass);
        }
        if (eventClass == null ? isRegistered(listener) : isRegistered(listener, eventClass)) {
            return Subscription.NOOP;
        }
        bindListener(listener, eventClass, false);
        return ownedSubscription(handlersForListener(listener, eventClass));
    }

    /**
     * Registers an object listener using a weak reference to prevent memory leaks.
     * When the listener is garbage collected, its handlers will automatically deactivate.
     *
     * <p><b>Warning:</b> do not pass an inline lambda here — nothing else
     * references it and it may be collected immediately. Keep the listener in
     * a field or variable that outlives the subscription.
     */
    public Subscription registerWeak(Object listener) {
        return registerWeak(listener, (Class<? extends Event>) null);
    }

    /**
     * Registers an object listener weakly for a specific event class.
     */
    public Subscription registerWeak(Object listener, Class<? extends Event> eventClass) {
        if (listener == null) {
            return Subscription.NOOP;
        }
        if (listener instanceof Consumer<?>) {
            if (eventClass == null) {
                return Subscription.NOOP;
            }
            @SuppressWarnings("unchecked")
            Consumer<Event> consumer = (Consumer<Event>) listener;
            @SuppressWarnings("unchecked")
            Class<Event> evt = (Class<Event>) eventClass;
            return registerWeak(evt, consumer);
        }
        if (listener instanceof Class<?>) {
            return register((Class<?>) listener, eventClass);
        }
        if (eventClass == null ? isRegistered(listener) : isRegistered(listener, eventClass)) {
            return Subscription.NOOP;
        }
        bindListener(listener, eventClass, true);
        return ownedSubscription(handlersForListener(listener, eventClass));
    }

    /**
     * Registers all static handlers of a listener class.
     */
    public Subscription register(Class<?> listenerClass) {
        return register(listenerClass, (Class<? extends Event>) null);
    }

    /**
     * Registers all static handlers of a listener class, optionally restricted
     * to the exact event class. Registering the same class again is a no-op.
     */
    public Subscription register(Class<?> listenerClass, Class<? extends Event> eventClass) {
        if (listenerClass == null) {
            return Subscription.NOOP;
        }
        if (isRegistered(listenerClass, eventClass)) {
            return Subscription.NOOP;
        }
        bindListenerPlan(listenerClass, listenerPlanFor(listenerClass), eventClass, true);
        return ownedSubscription(handlersForListener(listenerClass, eventClass));
    }

    private void bindListener(Object listener, Class<? extends Event> eventClass, boolean weak) {
        if (listener instanceof Class<?>) {
            Class<?> listenerClass = (Class<?>) listener;
            bindListenerPlan(listenerClass, listenerPlanFor(listenerClass), eventClass, true);
            return;
        }
        bindListenerPlan(listener, listenerPlanFor(listener.getClass()), eventClass, false, weak);
    }

    public Subscription register(Object listener, Method method) {
        if (listener == null || method == null) {
            return Subscription.NOOP;
        }
        Handler[] before = handlersForListener(listener, null);
        bindMatchingDefinitions(listener, method, null, listener instanceof Class<?>);
        return ownedSubscription(addedHandlers(before, handlersForListener(listener, null)));
    }

    public Subscription register(Object listener, Field field) {
        if (listener == null || field == null) {
            return Subscription.NOOP;
        }
        Handler[] before = handlersForListener(listener, null);
        bindMatchingDefinitions(listener, null, field, listener instanceof Class<?>);
        return ownedSubscription(addedHandlers(before, handlersForListener(listener, null)));
    }

    private static Handler[] addedHandlers(Handler[] before, Handler[] after) {
        int added = 0;
        outer:
        for (int i = 0; i < after.length; i++) {
            for (int k = 0; k < before.length; k++) {
                if (after[i] == before[k]) {
                    continue outer;
                }
            }
            added++;
        }
        if (added == 0) {
            return Handler.NO_HANDLERS;
        }
        Handler[] result = new Handler[added];
        int w = 0;
        outer2:
        for (int i = 0; i < after.length; i++) {
            for (int k = 0; k < before.length; k++) {
                if (after[i] == before[k]) {
                    continue outer2;
                }
            }
            result[w++] = after[i];
        }
        return result;
    }

    public <T extends Event> Subscription register(Class<T> eventType, Consumer<? super T> action) {
        if (action instanceof EventListener<?>) {
            @SuppressWarnings("unchecked")
            EventListener<? super T> listener = (EventListener<? super T>) action;
            return registerListener(eventType, listener);
        }
        return register(eventType, EventManager.DEFAULT_PRIORITY, false, action);
    }

    public <T extends Event> Subscription register(Class<T> eventType, int priority, Consumer<? super T> action) {
        return register(eventType, priority, false, action);
    }

    public <T extends Event> Subscription registerOnce(Class<T> eventType, Consumer<? super T> action) {
        return registerOnce(eventType, EventManager.DEFAULT_PRIORITY, false, action);
    }

    public <T extends Event> Subscription registerOnce(Class<T> eventType, int priority,
                                                       Consumer<? super T> action) {
        return registerOnce(eventType, priority, false, action);
    }

    public <T extends Event> Subscription registerOnce(final Class<T> eventType, int priority,
                                                       boolean ignoreCancelled,
                                                       final Consumer<? super T> action) {
        if (eventType == null || action == null) {
            return Subscription.NOOP;
        }

        final Handler handler = new Handler(
                action, null, null, null, eventType, normalizePriority(priority),
                ignoreCancelled, registrationOrder.getAndIncrement(), true, null,
                new ListenerIntrospection.Invoker() {
                    @Override
                    public void invoke(Event event) {
                        action.accept(eventType.cast(event));
                    }
                }
        );
        addHandler(handler);
        return new HandlerSubscription(handler);
    }

    public <T extends Event> Subscription registerFiltered(Class<T> eventType,
                                                            Predicate<? super T> filter,
                                                            Consumer<? super T> action) {
        return registerFiltered(eventType, EventManager.DEFAULT_PRIORITY, false, filter, action);
    }

    public <T extends Event> Subscription registerFiltered(Class<T> eventType, int priority,
                                                            Predicate<? super T> filter,
                                                            Consumer<? super T> action) {
        return registerFiltered(eventType, priority, false, filter, action);
    }

    public <T extends Event> Subscription registerFiltered(final Class<T> eventType, int priority,
                                                            boolean ignoreCancelled,
                                                            final Predicate<? super T> filter,
                                                            final Consumer<? super T> action) {
        if (eventType == null || filter == null || action == null) {
            return Subscription.NOOP;
        }
        final Handler handler = new Handler(
                action, null, null, null, eventType, normalizePriority(priority),
                ignoreCancelled, registrationOrder.getAndIncrement(), false,
                new Predicate<Event>() {
                    @Override
                    public boolean test(Event event) {
                        return filter.test(eventType.cast(event));
                    }
                },
                new ListenerIntrospection.Invoker() {
                    @Override
                    public void invoke(Event event) {
                        action.accept(eventType.cast(event));
                    }
                }
        );
        addHandler(handler);
        return new HandlerSubscription(handler);
    }

    public <T extends Event> Subscription register(final Class<T> eventType, int priority,
                                                   boolean ignoreCancelled, final Consumer<? super T> action) {
        if (eventType == null || action == null) {
            return Subscription.NOOP;
        }

        final Handler handler = new Handler(
                action,
                null,
                null,
                null,
                eventType,
                normalizePriority(priority),
                ignoreCancelled,
                registrationOrder.getAndIncrement(),
                false,
                null,
                new ListenerIntrospection.Invoker() {
                    @Override
                    public void invoke(Event event) {
                        action.accept(eventType.cast(event));
                    }
                }
        );
        addHandler(handler);
        return new HandlerSubscription(handler);
    }

    /**
     * Registers a {@link Consumer} handler that is referenced <em>weakly</em>:
     * the bus holds no strong reference to it, so once the caller drops all
     * references the handler is garbage collected and silently stops firing.
     *
     * <p><b>Warning:</b> do not pass an inline lambda here, e.g.
     * {@code registerWeak(Ping.class, e -> handle(e))}. A stateless lambda has
     * no other reference anywhere in the program and may be collected
     * immediately. Keep the consumer in a field or local variable that
     * outlives the subscription.
     *
     * @param eventType the event class to listen for
     * @param action    the weakly-referenced handler
     * @param <T>       the event type
     * @return a subscription handle for explicit unregistration
     */
    public <T extends Event> Subscription registerWeak(Class<T> eventType, Consumer<? super T> action) {
        if (eventType == null || action == null) {
            return Subscription.NOOP;
        }

        final WeakReference<Consumer<? super T>> weakRef = new WeakReference<Consumer<? super T>>(action);
        final Handler handler = new Handler(
                action,
                true,
                null,
                null,
                null,
                eventType,
                EventManager.DEFAULT_PRIORITY,
                false,
                registrationOrder.getAndIncrement(),
                false,
                null,
                new ListenerIntrospection.Invoker() {
                    @Override
                    public void invoke(Event event) {
                        Consumer<? super T> target = weakRef.get();
                        if (target != null) {
                            target.accept(eventType.cast(event));
                        }
                    }
                }
        );
        addHandler(handler);
        return new HandlerSubscription(handler);
    }

    public <T extends Event> Subscription registerListener(Class<T> eventType,
                                                           EventListener<? super T> listener) {
        if (eventType == null || listener == null) {
            return Subscription.NOOP;
        }
        return registerListener(eventType, listener.getPriority(), false, listener);
    }

    public <T extends Event> Subscription registerListener(Class<T> eventType, int priority,
                                                           EventListener<? super T> listener) {
        return registerListener(eventType, priority, false, listener);
    }

    public <T extends Event> Subscription registerListener(final Class<T> eventType, int priority,
                                                           boolean ignoreCancelled,
                                                           final EventListener<? super T> listener) {
        if (eventType == null || listener == null) {
            return Subscription.NOOP;
        }

        final Handler handler = new Handler(
                listener,
                null,
                null,
                null,
                eventType,
                normalizePriority(priority),
                ignoreCancelled,
                registrationOrder.getAndIncrement(),
                false,
                null,
                new ListenerIntrospection.Invoker() {
                    @SuppressWarnings({"unchecked", "rawtypes"})
                    @Override
                    public void invoke(Event event) {
                        ((EventListener) listener).onEvent(eventType.cast(event));
                    }
                }
        );
        addHandler(handler);
        return new HandlerSubscription(handler);
    }

    public <T extends Event> Subscription registerListenerOnce(Class<T> eventType,
                                                               EventListener<? super T> listener) {
        if (listener == null) {
            return Subscription.NOOP;
        }
        return registerListenerOnce(eventType, listener.getPriority(), false, listener);
    }

    public <T extends Event> Subscription registerListenerOnce(Class<T> eventType, int priority,
                                                               EventListener<? super T> listener) {
        return registerListenerOnce(eventType, priority, false, listener);
    }

    public <T extends Event> Subscription registerListenerOnce(final Class<T> eventType, int priority,
                                                               boolean ignoreCancelled,
                                                               final EventListener<? super T> listener) {
        if (eventType == null || listener == null) {
            return Subscription.NOOP;
        }
        final Handler handler = new Handler(
                listener, null, null, null, eventType, normalizePriority(priority),
                ignoreCancelled, registrationOrder.getAndIncrement(), true, null,
                new ListenerIntrospection.Invoker() {
                    @SuppressWarnings({"unchecked", "rawtypes"})
                    @Override
                    public void invoke(Event event) {
                        ((EventListener) listener).onEvent(eventType.cast(event));
                    }
                }
        );
        addHandler(handler);
        return new HandlerSubscription(handler);
    }

    @SafeVarargs
    public final <T extends Event> Subscription registerListener(
            EventListener<? super T> listener, Class<? extends T>... eventTypes) {
        if (listener == null) {
            return Subscription.NOOP;
        }
        return registerListener(listener.getPriority(), false, listener, eventTypes);
    }

    @SafeVarargs
    public final <T extends Event> Subscription registerListener(
            int priority, EventListener<? super T> listener, Class<? extends T>... eventTypes) {
        return registerListener(priority, false, listener, eventTypes);
    }

    @SafeVarargs
    public final <T extends Event> Subscription registerListener(
            int priority, boolean ignoreCancelled, EventListener<? super T> listener,
            Class<? extends T>... eventTypes) {
        if (listener == null || eventTypes == null || eventTypes.length == 0) {

            return Subscription.NOOP;
        }

        Set<Class<? extends T>> uniqueTypes = new LinkedHashSet<Class<? extends T>>();
        Collections.addAll(uniqueTypes, eventTypes);
        List<Subscription> subscriptions = new ArrayList<Subscription>(uniqueTypes.size());
        for (Class<? extends T> eventType : uniqueTypes) {
            if (eventType != null) {
                subscriptions.add(registerListener(eventType, priority, ignoreCancelled, listener));
            }
        }
        return Subscriptions.combine(subscriptions.toArray(new Subscription[subscriptions.size()]));
    }

    @SafeVarargs
    public final <T extends Event> Subscription register(
            Consumer<? super T> action, Class<? extends T>... eventTypes) {
        return register(EventManager.DEFAULT_PRIORITY, false, action, eventTypes);
    }

    @SafeVarargs
    public final <T extends Event> Subscription register(
            int priority, Consumer<? super T> action, Class<? extends T>... eventTypes) {
        return register(priority, false, action, eventTypes);
    }

    @SafeVarargs
    public final <T extends Event> Subscription register(
            int priority, boolean ignoreCancelled, Consumer<? super T> action,
            Class<? extends T>... eventTypes) {
        if (action == null || eventTypes == null || eventTypes.length == 0) {
            return Subscription.NOOP;
        }

        Set<Class<? extends T>> uniqueTypes = new LinkedHashSet<Class<? extends T>>();
        Collections.addAll(uniqueTypes, eventTypes);
        List<Subscription> subscriptions = new ArrayList<Subscription>(uniqueTypes.size());
        for (Class<? extends T> eventType : uniqueTypes) {
            if (eventType != null) {
                subscriptions.add(register(eventType, priority, ignoreCancelled, action));
            }
        }
        return Subscriptions.combine(subscriptions.toArray(new Subscription[subscriptions.size()]));
    }

    private Subscription ownedSubscription(Handler[] handlers) {
        return handlers.length == 0 ? Subscription.NOOP : new ListenerSubscription(handlers);
    }

    public void unregister(final Object listener) {
        if (listener == null) {
            return;
        }
        if (listener instanceof Class<?>) {
            unregister((Class<?>) listener);
            return;
        }
        removeHandlers(new Predicate<Handler>() {
            @Override
            public boolean test(Handler handler) {
                return handler.matchesListener(listener);
            }
        });
    }

    public void unregister(final Object listener, final Class<? extends Event> eventClass) {
        if (listener == null || eventClass == null) {
            return;
        }
        if (listener instanceof Class<?>) {
            unregister((Class<?>) listener, eventClass);
            return;
        }
        removeHandlers(eventClass, new Predicate<Handler>() {
            @Override
            public boolean test(Handler handler) {
                return handler.listener == listener && handler.eventType == eventClass;
            }
        });
    }

    public void unregister(final Object listener, final Method method) {
        if (listener == null || method == null) {
            return;
        }
        removeHandlers(new Predicate<Handler>() {
            @Override
            public boolean test(Handler handler) {
                return handler.listener == listener && sameMethod(handler.method, method);
            }
        });
    }

    public void unregister(final Object listener, final Field field) {
        if (listener == null || field == null) {
            return;
        }
        removeHandlers(new Predicate<Handler>() {
            @Override
            public boolean test(Handler handler) {
                return handler.listener == listener && sameField(handler.field, field);
            }
        });
    }

    public void unregister(final Class<?> listenerClass) {
        unregisterAssignable(listenerClass);
    }

    /**
     * Removes handlers owned by the class itself or by instances assignable to it.
     * This is the historical {@link #unregister(Class)} behavior.
     */
    public void unregisterAssignable(final Class<?> listenerClass) {
        if (listenerClass == null) {
            return;
        }
        removeHandlers(new Predicate<Handler>() {
            @Override
            public boolean test(Handler handler) {
                return handler.listener == listenerClass
                        || listenerClass.isAssignableFrom(listenerClassOf(handler.listener));
            }
        });
    }

    public void unregister(final Class<?> listenerClass, final Class<? extends Event> eventClass) {
        unregisterAssignable(listenerClass, eventClass);
    }

    /** Removes handlers for the exact event type owned by the class or its instances. */
    public void unregisterAssignable(final Class<?> listenerClass,
                                     final Class<? extends Event> eventClass) {
        if (listenerClass == null || eventClass == null) {
            return;
        }
        removeHandlers(eventClass, new Predicate<Handler>() {
            @Override
            public boolean test(Handler handler) {
                if (handler.eventType != eventClass) {
                    return false;
                }
                return handler.listener == listenerClass
                        || listenerClass.isAssignableFrom(listenerClassOf(handler.listener));
            }
        });
    }

    /** Removes only handlers whose owner is exactly this class or an instance of this class. */
    public void unregisterExact(final Class<?> listenerClass) {
        if (listenerClass == null) {
            return;
        }
        removeHandlers(new Predicate<Handler>() {
            @Override
            public boolean test(Handler handler) {
                return handler.listener == listenerClass
                        || (!(handler.listener instanceof Class<?>)
                        && listenerClass == handler.listener.getClass());
            }
        });
    }

    /** Removes only exact-event handlers whose owner is exactly this class or an instance of this class. */
    public void unregisterExact(final Class<?> listenerClass,
                                 final Class<? extends Event> eventClass) {
        if (listenerClass == null || eventClass == null) {
            return;
        }
        removeHandlers(eventClass, new Predicate<Handler>() {
            @Override
            public boolean test(Handler handler) {
                return handler.eventType == eventClass
                        && (handler.listener == listenerClass
                        || (!(handler.listener instanceof Class<?>)
                        && listenerClass == handler.listener.getClass()));
            }
        });
    }

    /**
     * Unregisters all handlers whose listener matches the supplied predicate.
     */
    public void unregisterIf(final Predicate<Object> listenerPredicate) {
        if (listenerPredicate == null) {
            return;
        }
        removeHandlers(new Predicate<Handler>() {
            @Override
            public boolean test(Handler handler) {
                Object l = handler.getListener();
                return l != null && listenerPredicate.test(l);
            }
        });
    }

    /**
     * Unregisters all handlers registered for the specified event type.
     */
    public void unregisterEventType(final Class<? extends Event> eventType) {
        if (eventType == null) {
            return;
        }
        removeHandlers(eventType, new Predicate<Handler>() {
            @Override
            public boolean test(Handler handler) {
                return handler.eventType == eventType;
            }
        });
    }

    public void unregisterAll() {
        clear();
    }

    /**
     * Actively scans all registered event types and purges any handlers whose
     * weak listener targets have been garbage collected.
     *
     * @return the total count of dead handlers purged from the bus
     */
    public int purgeDeadHandlers() {
        final AtomicInteger purged = new AtomicInteger();
        final AtomicInteger weakFound = new AtomicInteger();
        for (Class<? extends Event> eventType : eventHandlers.keySet()) {
            removeHandlers(eventType, new Predicate<Handler>() {
                @Override
                public boolean test(Handler handler) {
                    if (handler.isWeak()) {
                        weakFound.incrementAndGet();
                        if (handler.isDead()) {
                            purged.incrementAndGet();
                            return true;
                        }
                    }
                    return false;
                }
            });
        }
        // Self-correct the fast-path flag: if no weak handlers remain at all,
        // stop paying for the periodic scan until one is registered again.
        if (weakFound.get() == 0) {
            hasWeakHandlers = false;
        }
        return purged.get();
    }

    /**
     * Opportunistically reaps garbage-collected weak handlers. The scan is
     * amortized: it runs at most once per 1024 dispatches, and only when at
     * least one weak handler was registered since the last scan found none.
     */
    void maybePurgeDeadHandlers() {
        if (hasWeakHandlers && (dispatchesSinceWeakPurge.incrementAndGet() & 1023) == 0) {
            purgeDeadHandlers();
        }
    }

    public void removeEntry(Class<? extends Event> eventType) {
        if (eventType == null) {
            return;
        }
        Handler[] removed = eventHandlers.remove(eventType);
        if (removed != null) {
            for (Handler handler : removed) {
                handler.active.set(false);
            }
        }
        // Always invalidate: dispatch warms dispatchCache even with no handler,
        // and the entry keeps a strong ref to the event Class, blocking unload.
        invalidateDispatchCache(eventType);
    }

    public boolean isRegistered(Object listener) {
        if (listener == null) {
            return false;
        }
        if (listener instanceof Class<?>) {
            return isRegistered((Class<?>) listener);
        }
        for (Handler[] handlers : eventHandlers.values()) {
            for (int i = 0; i < handlers.length; i++) {
                Handler handler = handlers[i];
                if (handler.active.get() && !handler.isDead() && handler.matchesListener(listener)) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean isRegistered(Object listener, Class<? extends Event> eventClass) {
        if (listener == null || eventClass == null) {
            return false;
        }
        Handler[] handlers = eventHandlers.get(eventClass);
        if (handlers == null) {
            return false;
        }
        if (listener instanceof Class<?>) {
            Class<?> listenerClass = (Class<?>) listener;
            for (int i = 0; i < handlers.length; i++) {
                Handler handler = handlers[i];
                if (handler.active.get() && !handler.isDead()
                        && (handler.listener == listenerClass
                        || listenerClass.isAssignableFrom(listenerClassOf(handler.listener)))) {
                    return true;
                }
            }
            return false;
        }
        for (int i = 0; i < handlers.length; i++) {
            Handler handler = handlers[i];
            if (handler.active.get() && !handler.isDead() && handler.matchesListener(listener)) {
                return true;
            }
        }
        return false;
    }

    public boolean isRegistered(Class<?> listenerClass) {
        if (listenerClass == null) {
            return false;
        }
        for (Handler[] handlers : eventHandlers.values()) {
            for (int i = 0; i < handlers.length; i++) {
                Handler handler = handlers[i];
                if (handler.active.get() && !handler.isDead()
                        && (handler.listener == listenerClass
                        || listenerClass.isAssignableFrom(listenerClassOf(handler.listener)))) {
                    return true;
                }
            }
        }
        return false;
    }

    public int listenerCount() {
        Set<Object> listeners = Collections.newSetFromMap(new IdentityHashMap<Object, Boolean>());
        for (Handler[] handlers : eventHandlers.values()) {
            for (int i = 0; i < handlers.length; i++) {
                Handler handler = handlers[i];
                if (handler.active.get() && !handler.isDead()) {
                    listeners.add(handler.getListener());
                }
            }
        }
        return listeners.size();
    }

    public int handlerCount() {
        int count = 0;
        for (Handler[] handlers : eventHandlers.values()) {
            for (int i = 0; i < handlers.length; i++) {
                Handler handler = handlers[i];
                if (handler.active.get() && !handler.isDead()) {
                    count++;
                }
            }
        }
        return count;
    }

    /** Returns a stable, read-only snapshot of event types with registered handlers. */
    public Set<Class<? extends Event>> registeredEventTypes() {
        if (eventHandlers.isEmpty()) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(new HashSet<Class<? extends Event>>(eventHandlers.keySet()));
    }

    /** Returns whether this bus currently has no registered handlers. */
    public boolean isEmpty() {
        return eventHandlers.isEmpty();
    }

    public int handlerCount(Class<? extends Event> eventType) {
        return eventType == null ? 0 : handlersFor(eventType).length;
    }

    public int exactHandlerCount(Class<? extends Event> eventType) {
        return eventType == null ? 0 : exactHandlersFor(eventType).length;
    }

    public boolean hasListeners(Class<? extends Event> eventType) {
        if (eventType == null) {
            return false;
        }
        Handler[] handlers = handlersFor(eventType);
        for (int i = 0; i < handlers.length; i++) {
            Handler handler = handlers[i];
            if (handler.active.get() && !handler.isDead()) {
                return true;
            }
        }
        return false;
    }

    public boolean hasExactListeners(Class<? extends Event> eventType) {
        if (eventType == null) {
            return false;
        }
        Handler[] handlers = exactHandlersFor(eventType);
        for (int i = 0; i < handlers.length; i++) {
            Handler handler = handlers[i];
            if (handler.active.get() && !handler.isDead()) {
                return true;
            }
        }
        return false;
    }

    Handler[] handlersFor(Class<?> eventClass) {
        CachedDispatch cached = dispatchCache.get(eventClass);
        if (cached != null) {
            return cached.handlers;
        }

        long startVersion = mutationVersion.get();
        Handler[] built = buildDispatchList(eventClass);
        CachedDispatch entry = new CachedDispatch(built);
        dispatchCache.put(eventClass, entry);
        // Validate-after-publish: a registration racing this build could
        // otherwise leave a stale list cached indefinitely.
        Class<? extends Event>[] hierarchy = EVENT_HIERARCHIES.get(eventClass);
        for (int i = 0; i < hierarchy.length; i++) {
            Long stamp = typeMutationStamps.get(hierarchy[i]);
            if (stamp != null && stamp > startVersion) {
                dispatchCache.remove(eventClass, entry);
                break;
            }
        }
        return built;
    }

    /**
     * Marks the handlers of {@code mutatedType} as changed and drops only the
     * cache entries whose dispatch list can be affected by that type, leaving
     * unrelated event types cached.
     */
    private void invalidateDispatchCache(Class<? extends Event> mutatedType) {
        typeMutationStamps.put(mutatedType, mutationVersion.incrementAndGet());
        for (Class<?> cachedType : dispatchCache.keySet()) {
            if (cachedType == mutatedType) {
                dispatchCache.remove(cachedType);
                continue;
            }
            Class<? extends Event>[] hierarchy = EVENT_HIERARCHIES.get(cachedType);
            for (int i = 0; i < hierarchy.length; i++) {
                if (hierarchy[i] == mutatedType) {
                    dispatchCache.remove(cachedType);
                    break;
                }
            }
        }
    }

    Handler[] exactHandlersFor(Class<?> eventClass) {
        Handler[] handlers = eventHandlers.get(eventClass);
        return handlers == null ? Handler.NO_HANDLERS : handlers;
    }

    private static void collectEventHierarchy(Class<?> type, Set<Class<? extends Event>> collected) {
        if (type == null || type == Object.class || !Event.class.isAssignableFrom(type)) {
            return;
        }
        @SuppressWarnings("unchecked")
        Class<? extends Event> eventType = (Class<? extends Event>) type;
        if (!collected.add(eventType)) {
            return;
        }
        for (Class<?> iface : type.getInterfaces()) {
            collectEventHierarchy(iface, collected);
        }
        collectEventHierarchy(type.getSuperclass(), collected);
    }

    private Handler[] buildDispatchList(Class<?> eventClass) {
        Class<? extends Event>[] hierarchy = EVENT_HIERARCHIES.get(eventClass);
        if (hierarchy.length == 0) {
            return Handler.NO_HANDLERS;
        }

        int matchingTypes = 0;
        Class<? extends Event> singleType = null;
        for (int i = 0; i < hierarchy.length; i++) {
            if (eventHandlers.containsKey(hierarchy[i])) {
                matchingTypes++;
                singleType = hierarchy[i];
            }
        }

        if (matchingTypes == 0) {
            return Handler.NO_HANDLERS;
        }
        if (matchingTypes == 1) {
            Handler[] handlers = eventHandlers.get(singleType);
            return (handlers == null || handlers.length == 0) ? Handler.NO_HANDLERS : handlers;
        }

        LinkedHashSet<Handler> collected = new LinkedHashSet<Handler>();
        for (int i = 0; i < hierarchy.length; i++) {
            Handler[] handlers = eventHandlers.get(hierarchy[i]);
            if (handlers != null) {
                for (int j = 0; j < handlers.length; j++) {
                    collected.add(handlers[j]);
                }
            }
        }

        if (collected.isEmpty()) {
            return Handler.NO_HANDLERS;
        }

        List<Handler> handlers = new ArrayList<Handler>(collected);
        return HandlerOrdering.sortWithDag(handlers);
    }

    static int normalizePriority(int priority) {
        return priority == Priority.UNSPECIFIED ? EventManager.DEFAULT_PRIORITY : priority;
    }

    private static ListenerIntrospection.ListenerPlan listenerPlanFor(final Class<?> listenerClass) {
        return ListenerIntrospection.planFor(listenerClass);
    }

    private static ListenerIntrospection.Invoker invokerFor(Method method, Object listener) {
        return ListenerIntrospection.invokerFor(method, listener);
    }

    private static ListenerIntrospection.InvokerFactory invokerFactoryFor(Method method) {
        return ListenerIntrospection.invokerFactoryFor(method);
    }

    private static EventListener<?> listenerFromField(Object listener, Field field, boolean reportFailure) {
        return ListenerIntrospection.listenerFromField(listener, field, reportFailure);
    }

    private void bindMatchingDefinitions(Object listener, Method method, Field field, boolean staticOnly) {
        ListenerIntrospection.ListenerPlan plan = listenerPlanFor(listener instanceof Class<?>
                ? (Class<?>) listener
                : listener.getClass());
        for (int i = 0; i < plan.definitions.length; i++) {
            ListenerIntrospection.HandlerDefinition definition = plan.definitions[i];
            if (method != null) {
                if (definition.method == null || !sameMethod(definition.method, method)) {
                    continue;
                }
            } else if (field != null) {
                if (definition.field == null || !sameField(definition.field, field)) {
                    continue;
                }
            } else {
                continue;
            }
            bindDefinition(listener, definition, staticOnly);
        }
    }

    private void bindListenerPlan(Object listener, ListenerIntrospection.ListenerPlan plan, Class<? extends Event> eventClass,
                                  boolean staticOnly) {
        bindListenerPlan(listener, plan, eventClass, staticOnly, false);
    }

    private void bindListenerPlan(Object listener, ListenerIntrospection.ListenerPlan plan, Class<? extends Event> eventClass,
                                  boolean staticOnly, boolean weak) {
        for (ListenerIntrospection.HandlerDefinition definition : plan.definitions) {
            if (eventClass != null && definition.eventType != eventClass) {
                continue;
            }
            bindDefinition(listener, definition, staticOnly, weak);
        }
    }

    private void bindDefinition(final Object listener, final ListenerIntrospection.HandlerDefinition definition, boolean staticOnly) {
        bindDefinition(listener, definition, staticOnly, false);
    }

    private void bindDefinition(final Object listener, final ListenerIntrospection.HandlerDefinition definition, boolean staticOnly, final boolean weak) {
        if (staticOnly && !definition.staticMember) {
            return;
        }

        if (definition.method != null) {
            Method method = definition.method;
            ListenerIntrospection.Invoker invoker;
            if (weak && !definition.staticMember) {
                final WeakReference<Object> weakRef = new WeakReference<Object>(listener);
                final ListenerIntrospection.InvokerFactory factory = invokerFactoryFor(method);
                invoker = new ListenerIntrospection.Invoker() {
                    @Override
                    public void invoke(Event event) throws Throwable {
                        Object target = weakRef.get();
                        if (target != null) {
                            factory.create(target).invoke(event);
                        }
                    }
                };
            } else {
                invoker = invokerFor(method, listener);
            }
            Handler handler = new Handler(
                    listener,
                    weak,
                    method,
                    null,
                    definition.staticMember ? method.getDeclaringClass() : listener,
                    definition.eventType,
                    normalizePriority(definition.priority),
                    definition.ignoreCancelled,
                    registrationOrder.getAndIncrement(), false, definition.filter,
                    definition.genericType,
                    definition.id,
                    definition.after,
                    definition.before,
                    definition.afterClasses,
                    definition.beforeClasses,
                    invoker
            );
            addHandler(handler);
            if (definition.sticky) {
                owner.replaySticky(definition.eventType, definition.genericType, handler);
            }
            return;
        }

        final Field field = definition.field;
        EventListener<?> fieldListener = listenerFromField(listener, field, true);
        if (fieldListener == null) {
            return;
        }

        final Class<? extends Event> dispatchType = definition.eventType;
        int priority = definition.listenerPriority
                ? listenerPriority(fieldListener)
                : definition.priority;

        final ListenerIntrospection.Invoker invoker;
        final WeakReference<Object> weakRef = (weak && !definition.staticMember) ? new WeakReference<Object>(listener) : null;
        if (Modifier.isFinal(field.getModifiers())) {
            @SuppressWarnings("unchecked")
            final EventListener<Event> directListener = (EventListener<Event>) fieldListener;
            if (weakRef != null) {
                invoker = new ListenerIntrospection.Invoker() {
                    @Override
                    public void invoke(Event event) {
                        if (weakRef.get() != null) {
                            directListener.onEvent(dispatchType.cast(event));
                        }
                    }
                };
            } else {
                invoker = new ListenerIntrospection.Invoker() {
                    @Override
                    public void invoke(Event event) {
                        directListener.onEvent(dispatchType.cast(event));
                    }
                };
            }
        } else {
            if (weakRef != null) {
                invoker = new ListenerIntrospection.Invoker() {
                    @Override
                    public void invoke(Event event) {
                        Object target = weakRef.get();
                        if (target != null) {
                            EventListener<?> current = listenerFromField(target, field, false);
                            if (current != null) {
                                @SuppressWarnings("unchecked")
                                EventListener<Event> currentListener = (EventListener<Event>) current;
                                currentListener.onEvent(dispatchType.cast(event));
                            }
                        }
                    }
                };
            } else {
                invoker = new ListenerIntrospection.Invoker() {
                    @Override
                    public void invoke(Event event) {
                        EventListener<?> current = listenerFromField(listener, field, false);
                        if (current != null) {
                            @SuppressWarnings("unchecked")
                            EventListener<Event> currentListener = (EventListener<Event>) current;
                            currentListener.onEvent(dispatchType.cast(event));
                        }
                    }
                };
            }
        }

        Handler handler = new Handler(
                listener,
                weak,
                null,
                field,
                definition.staticMember ? field.getDeclaringClass() : listener,
                dispatchType,
                normalizePriority(priority),
                definition.ignoreCancelled,
                registrationOrder.getAndIncrement(), false, definition.filter,
                definition.genericType,
                definition.id,
                definition.after,
                definition.before,
                definition.afterClasses,
                definition.beforeClasses,
                invoker
        );
        addHandler(handler);
        if (definition.sticky) {
            owner.replaySticky(definition.eventType, definition.genericType, handler);
        }
    }

    void addHandler(final Handler handler) {
        // Single choke point for every public register* variant and the
        // on()/subscribe()/expect() builder paths: registering on a closed
        // bus is a programming error, fail fast.
        owner.ensureOpen();
        if (handler.isWeak()) {
            // Publish the flag before the handler becomes visible so a racing
            // opportunistic purge is less likely to miss it.
            hasWeakHandlers = true;
        }
        boolean added = false;
        while (true) {
            Handler[] existing = eventHandlers.get(handler.eventType);
            Handler[] updated = insertHandler(existing, handler);
            if (updated == existing) {
                return;
            }
            if (existing == null) {
                if (eventHandlers.putIfAbsent(handler.eventType, updated) == null) {
                    added = true;
                    break;
                }
            } else {
                if (eventHandlers.replace(handler.eventType, existing, updated)) {
                    added = true;
                    break;
                }
            }
        }
        if (added) {
            invalidateDispatchCache(handler.eventType);
        }
    }

    private Handler[] handlersForListener(Object listener, Class<? extends Event> eventClass) {
        List<Handler> matching = new ArrayList<Handler>();
        if (eventClass != null) {
            Handler[] handlers = eventHandlers.get(eventClass);
            if (handlers != null) {
                for (Handler handler : handlers) {
                    if (handler.matchesListener(listener)) {
                        matching.add(handler);
                    }
                }
            }
        } else {
            for (Handler[] handlers : eventHandlers.values()) {
                for (Handler handler : handlers) {
                    if (handler.matchesListener(listener)) {
                        matching.add(handler);
                    }
                }
            }
        }
        return matching.toArray(new Handler[matching.size()]);
    }

    void removeHandlers(Class<? extends Event> targetType, final Predicate<Handler> predicate) {
        if (targetType == null) {
            removeHandlers(predicate);
            return;
        }
        boolean removed = false;
        while (true) {
            Handler[] handlers = eventHandlers.get(targetType);
            if (handlers == null) {
                return;
            }
            Handler[] updated = removeMatching(handlers, predicate);
            if (updated == handlers) {
                return;
            }
            if (updated.length == 0) {
                if (eventHandlers.remove(targetType, handlers)) {
                    removed = true;
                    markInactive(handlers, predicate);
                    break;
                }
            } else {
                if (eventHandlers.replace(targetType, handlers, updated)) {
                    removed = true;
                    markInactive(handlers, predicate);
                    break;
                }
            }
        }
        if (removed) {
            invalidateDispatchCache(targetType);
        }
    }

    private void removeHandlers(final Predicate<Handler> predicate) {
        List<Class<? extends Event>> mutated = null;
        for (Class<? extends Event> eventType : eventHandlers.keySet()) {
            while (true) {
                Handler[] handlers = eventHandlers.get(eventType);
                if (handlers == null) {
                    break;
                }
                Handler[] updated = removeMatching(handlers, predicate);
                if (updated == handlers) {
                    break;
                }
                boolean replaced;
                if (updated.length == 0) {
                    replaced = eventHandlers.remove(eventType, handlers);
                } else {
                    replaced = eventHandlers.replace(eventType, handlers, updated);
                }
                if (replaced) {
                    markInactive(handlers, predicate);
                    if (mutated == null) {
                        mutated = new ArrayList<Class<? extends Event>>();
                    }
                    mutated.add(eventType);
                    break;
                }
            }
        }
        if (mutated != null) {
            for (int i = 0; i < mutated.size(); i++) {
                invalidateDispatchCache(mutated.get(i));
            }
        }
    }

    private static void markInactive(Handler[] handlers, Predicate<Handler> predicate) {
        for (int i = 0; i < handlers.length; i++) {
            if (predicate.test(handlers[i])) {
                handlers[i].active.set(false);
            }
        }
    }

    private static int listenerPriority(EventListener<?> listener) {
        try {
            return listener.getPriority();
        } catch (Throwable t) {
            log.log(Level.WARNING, "EventListener.getPriority() failed; using normal priority", t);
            return EventManager.DEFAULT_PRIORITY;
        }
    }

    private static Class<?> listenerClassOf(Object listener) {
        return listener instanceof Class<?> ? (Class<?>) listener : listener.getClass();
    }

    private static boolean sameMethod(Method left, Method right) {
        if (left == null || right == null) {
            return false;
        }
        if (left.equals(right)) {
            return true;
        }
        return left.getDeclaringClass() == right.getDeclaringClass()
                && left.getReturnType() == right.getReturnType()
                && left.getName().equals(right.getName())
                && java.util.Arrays.equals(left.getParameterTypes(), right.getParameterTypes());
    }

    private static boolean sameField(Field left, Field right) {
        if (left == null || right == null) {
            return false;
        }
        if (left.equals(right)) {
            return true;
        }
        return left.getDeclaringClass() == right.getDeclaringClass()
                && left.getName().equals(right.getName()) && left.getType() == right.getType();
    }

    private static Handler[] insertHandler(Handler[] current, Handler handler) {
        Handler[] handlers = current == null ? Handler.NO_HANDLERS : current;
        for (int i = 0; i < handlers.length; i++) {
            if (handlers[i].equals(handler)) {
                return handlers;
            }
        }
        List<Handler> list = new ArrayList<Handler>(handlers.length + 1);
        for (int i = 0; i < handlers.length; i++) {
            list.add(handlers[i]);
        }
        list.add(handler);
        return HandlerOrdering.sortWithDag(list);
    }

    private static Handler[] removeMatching(Handler[] handlers, Predicate<Handler> predicate) {
        int remaining = 0;
        for (int i = 0; i < handlers.length; i++) {
            if (!predicate.test(handlers[i])) {
                remaining++;
            }
        }
        if (remaining == handlers.length) {
            return handlers;
        }
        if (remaining == 0) {
            return Handler.NO_HANDLERS;
        }

        Handler[] updated = new Handler[remaining];
        int index = 0;
        for (int i = 0; i < handlers.length; i++) {
            if (!predicate.test(handlers[i])) {
                updated[index++] = handlers[i];
            }
        }
        return updated;
    }

    /**
     * Deactivates and drops all registrations. Returns true if any
     * registrations or warmed cache entries existed.
     */
    void clear() {
        boolean hadRegistrations = !eventHandlers.isEmpty() || !dispatchCache.isEmpty();
        for (Handler[] handlers : eventHandlers.values()) {
            for (Handler handler : handlers) {
                handler.active.set(false);
            }
        }
        eventHandlers.clear();
        dispatchCache.clear();
        typeMutationStamps.clear();
        hasWeakHandlers = false;
        dispatchesSinceWeakPurge.set(0);
        if (hadRegistrations) {
            mutationVersion.incrementAndGet();
        }
    }

    /** Number of event types with at least one registration. */
    int registeredTypeCount() {
        return eventHandlers.size();
    }

    /** Next value of the global registration sequence. */
    long nextRegistrationOrder() {
        return registrationOrder.getAndIncrement();
    }

    /** Creates the subscription handle for a single handler. */
    Subscription newHandlerSubscription(Handler handler) {
        return new HandlerSubscription(handler);
    }

    /** Snapshot of listener names per event type, for topology export. */
    Map<Class<? extends Event>, List<String>> listenerTopologySnapshot() {
        Map<Class<? extends Event>, List<String>> result = new LinkedHashMap<Class<? extends Event>, List<String>>();
        for (Map.Entry<Class<? extends Event>, Handler[]> entry : eventHandlers.entrySet()) {
            List<String> names = new ArrayList<String>();
            Handler[] handlers = entry.getValue();
            if (handlers != null) {
                for (Handler h : handlers) {
                    String name = (h.id != null && !h.id.isEmpty()) ? h.id :
                            (h.getListenerClass() != null ? h.getListenerClass().getSimpleName() : "Handler@" + h.order);
                    names.add(name);
                }
            }
            result.put(entry.getKey(), names);
        }
        return result;
    }

    Map<Class<? extends Event>, List<String>> getListenerTopologySnapshot() {
        Map<Class<? extends Event>, List<String>> result = new java.util.LinkedHashMap<Class<? extends Event>, List<String>>();
        for (Map.Entry<Class<? extends Event>, Handler[]> entry : eventHandlers.entrySet()) {
            List<String> names = new ArrayList<String>();
            Handler[] handlers = entry.getValue();
            if (handlers != null) {
                for (Handler h : handlers) {
                    String name = (h.id != null && !h.id.isEmpty()) ? h.id :
                            (h.getListenerClass() != null ? h.getListenerClass().getSimpleName() : "Handler@" + h.order);
                    names.add(name);
                }
            }
            result.put(entry.getKey(), names);
        }
        return result;
    }

    <T extends Event> Subscription registerSubscriber(
            final Class<T> eventType,
            int priority,
            boolean ignoreCancelled,
            final boolean once,
            final boolean weak,
            final boolean sticky,
            final Predicate<? super T> filter,
            final Type genericType,
            final Consumer<? super T> action) {
        return registerSubscriber(
                eventType, priority, ignoreCancelled, once, weak, sticky, filter, genericType,
                "", null, null, null, null, action
        );
    }

    <T extends Event> Subscription registerSubscriber(
            final Class<T> eventType,
            int priority,
            boolean ignoreCancelled,
            final boolean once,
            final boolean weak,
            final boolean sticky,
            final Predicate<? super T> filter,
            final Type genericType,
            final String id,
            final String[] after,
            final String[] before,
            final Class<?>[] afterClasses,
            final Class<?>[] beforeClasses,
            final Consumer<? super T> action) {
        if (eventType == null || action == null) {
            return Subscription.NOOP;
        }
        final Predicate<Event> eventFilter = filter == null ? null : new Predicate<Event>() {
            @Override
            public boolean test(Event event) {
                return filter.test(eventType.cast(event));
            }
        };
        final ListenerIntrospection.Invoker invoker = new ListenerIntrospection.Invoker() {
            @Override
            public void invoke(Event event) throws Throwable {
                action.accept(eventType.cast(event));
            }
        };
        final Handler handler = new Handler(
                action, weak, null, null, null, eventType, normalizePriority(priority),
                ignoreCancelled, registrationOrder.getAndIncrement(), once, eventFilter, genericType,
                id, after, before, afterClasses, beforeClasses, invoker
        );
        addHandler(handler);
        if (sticky) {
            owner.replaySticky(eventType, genericType, handler);
        }
        return new HandlerSubscription(handler);
    }
}
