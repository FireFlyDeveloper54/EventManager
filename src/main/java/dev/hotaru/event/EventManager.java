package dev.hotaru.event;

import dev.hotaru.event.annotations.EventTarget;
import dev.hotaru.event.impl.Cancellable;
import dev.hotaru.event.impl.Event;
import dev.hotaru.event.impl.Stoppable;

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

public class EventManager implements AutoCloseable {
    private static final Logger log = Logger.getLogger(EventManager.class.getName());
    private static final int DEFAULT_PRIORITY = Priority.NORMAL;
    static final Handler[] NO_HANDLERS = new Handler[0];
    private static final EventErrorHandler DEFAULT_ERROR_HANDLER = new EventErrorHandler() {
        @Override
        public void handle(Event event, Object listener, Throwable throwable) {
            log.log(Level.SEVERE, "Failed to dispatch " + event.getClass().getName()
                    + " to listener " + listener, throwable);
        }
    };
    private static final Comparator<Handler> HANDLER_ORDER = HandlerOrdering.HANDLER_ORDER;

        private final ConcurrentMap<Class<?>, List<EventUpcaster.Typed<?, ?>>> upcasters =
            new ConcurrentHashMap<Class<?>, List<EventUpcaster.Typed<?, ?>>>();
    // Hot-path guard: most buses never register an upcaster; this flag lets
    // dispatch skip the per-event map lookup entirely in that common case.
    private volatile boolean hasUpcasters = false;
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
    private final String name;
    private final Executor defaultExecutor;
    private final AtomicLong registrationOrder = new AtomicLong();
    private final AtomicLong mutationVersion = new AtomicLong();
    private final LongAdder dispatchedEvents = new LongAdder();
    private final LongAdder handlerInvocations = new LongAdder();
    private final LongAdder failures = new LongAdder();
    private final LongAdder totalDurationNanos = new LongAdder();
    private final AtomicLong maxDurationNanos = new AtomicLong();
    private volatile boolean deadEventsEnabled = true;
    private volatile boolean closed = false;
    private volatile Predicate<Thread> threadEnforcer;
    private final EventManager parent;
    private final Set<EventManager> children = Collections.newSetFromMap(new ConcurrentHashMap<EventManager, Boolean>());
    private final ConcurrentMap<Class<? extends Event>, Event> stickyEvents =
            new ConcurrentHashMap<Class<? extends Event>, Event>();

    // Weak keys so per-type metrics never pin an event class (or its class
    // loader) after the type becomes unreachable. Compound check-then-act
    // sequences synchronize on this map explicitly; see counterFor.
    private final Map<Class<?>, MetricCounter> metricsByType =
            Collections.synchronizedMap(new WeakHashMap<Class<?>, MetricCounter>());

    // Drives the amortized auto-purge of garbage-collected weak handlers:
    // set when a weak handler is registered, cleared by purgeDeadHandlers
    // when a scan finds no weak handlers left at all.
    private volatile boolean hasWeakHandlers = false;
    // Counts dispatches towards the next opportunistic weak-handler purge.
    private final AtomicLong dispatchesSinceWeakPurge = new AtomicLong();


    private volatile boolean metricsEnabled = true;
    private volatile EventErrorHandler errorHandler = DEFAULT_ERROR_HANDLER;
    private volatile ErrorPolicy errorPolicy = ErrorPolicy.CONTINUE;
    private volatile EventInterceptor interceptor;
    private final List<EventInterceptor> interceptorList = new CopyOnWriteArrayList<EventInterceptor>();

    public EventManager() {
        this("EventManager", null, null, null, null);
    }

    public EventManager(String name) {
        this(name, null, null, null, null);
    }

    public EventManager(EventManager parent) {
        this(parent != null ? parent.getName() + "-child" : "EventManager-child", parent, null, null, null);
    }

    public EventManager(EventErrorHandler errorHandler) {
        this("EventManager", null, errorHandler, null, null);
    }

    public EventManager(EventErrorHandler errorHandler, ErrorPolicy errorPolicy) {
        this("EventManager", null, errorHandler, errorPolicy, null);
    }

    public EventManager(EventManager parent, EventErrorHandler errorHandler, ErrorPolicy errorPolicy) {
        this(parent != null ? parent.getName() + "-child" : "EventManager-child", parent, errorHandler, errorPolicy, null);
    }

    public EventManager(String name, EventManager parent, EventErrorHandler errorHandler, ErrorPolicy errorPolicy, Executor defaultExecutor) {
        this.name = (name != null && !name.trim().isEmpty()) ? name.trim() : "EventManager";
        this.parent = parent;
        this.errorHandler = errorHandler != null ? errorHandler : DEFAULT_ERROR_HANDLER;
        this.errorPolicy = errorPolicy != null ? errorPolicy : ErrorPolicy.CONTINUE;
        this.defaultExecutor = defaultExecutor != null ? defaultExecutor : ForkJoinPool.commonPool();
    }

    public String getName() {
        return name;
    }

    public Executor getDefaultExecutor() {
        return defaultExecutor;
    }

    /**
     * Creates a scoped child event bus.
     * Events dispatched on the child bus will trigger child listeners first,
     * then bubble up to this parent bus (unless cancelled or stopped).
     */
    public EventManager createChildBus() {
        return createChildBus(this.name + "-child");
    }

    public EventManager createChildBus(String childName) {
        if (closed) {
            throw new IllegalStateException("Cannot create child bus from closed parent event manager");
        }
        EventManager child = new EventManager(childName, this, this.errorHandler, this.errorPolicy, this.defaultExecutor);
        child.setMetricsEnabled(this.metricsEnabled);
        child.setDeadEventsEnabled(this.deadEventsEnabled);
        children.add(child);
        return child;
    }

    public EventManager getParent() {
        return parent;
    }

    public Set<EventManager> getChildren() {
        return Collections.unmodifiableSet(new HashSet<EventManager>(children));
    }

    public boolean isMetricsEnabled() {
        return metricsEnabled;
    }

    public void setMetricsEnabled(boolean metricsEnabled) {
        this.metricsEnabled = metricsEnabled;
    }

    void setErrorHandler(EventErrorHandler errorHandler) {
        this.errorHandler = errorHandler != null ? errorHandler : DEFAULT_ERROR_HANDLER;
    }

    public EventErrorHandler getErrorHandler() {
        return errorHandler;
    }

    /**
     * Routes a handler failure that occurred outside the normal dispatch
     * stack (scheduled debounce/batch flushes) to the bus's pluggable
     * {@link EventErrorHandler}. The error handler itself is invoked in a
     * fail-safe wrapper: if it throws, both it and the original failure are
     * logged instead of being lost to the scheduler.
     */
    static void notifyHandlerFailure(EventManager bus, Event event, Object listener, Throwable failure) {
        EventErrorHandler handler = bus != null ? bus.errorHandler : DEFAULT_ERROR_HANDLER;
        try {
            handler.handle(event, listener, failure);
        } catch (Throwable secondary) {
            log.log(Level.SEVERE, "EventErrorHandler itself failed while reporting a handler failure", secondary);
            log.log(Level.SEVERE, "Original handler failure", failure);
        }
    }

    public ErrorPolicy getErrorPolicy() {
        return errorPolicy;
    }

    void setErrorPolicy(ErrorPolicy errorPolicy) {
        this.errorPolicy = errorPolicy != null ? errorPolicy : ErrorPolicy.CONTINUE;
    }

    public EventInterceptor getInterceptor() {
        return interceptor;
    }

    public void setInterceptor(EventInterceptor interceptor) {
        this.interceptorList.clear();
        if (interceptor != null) {
            this.interceptorList.add(interceptor);
        }
        this.interceptor = interceptor;
    }

    /**
     * Appends an interceptor to the current interceptor chain.
     */
    public void addInterceptor(EventInterceptor interceptor) {
        if (interceptor == null) {
            return;
        }
        ensureOpen();
        this.interceptorList.add(interceptor);
        this.interceptor = EventInterceptors.chain(this.interceptorList);
    }

    /**
     * Removes an interceptor from the interceptor chain.
     *
     * @param interceptor the interceptor to remove
     * @return true if removed, false otherwise
     */
    public boolean removeInterceptor(EventInterceptor interceptor) {
        if (interceptor == null) {
            return false;
        }
        boolean removed = this.interceptorList.remove(interceptor);
        if (removed) {
            this.interceptor = EventInterceptors.chain(this.interceptorList);
        }
        return removed;
    }

    public void enforceThread(final Thread expectedThread) {
        if (expectedThread == null) {
            this.threadEnforcer = null;
        } else {
            this.threadEnforcer = new Predicate<Thread>() {
                @Override
                public boolean test(Thread thread) {
                    return thread == expectedThread;
                }
            };
        }
    }

    public void enforceThread(Predicate<Thread> threadEnforcer) {
        this.threadEnforcer = threadEnforcer;
    }

    public Predicate<Thread> getThreadEnforcer() {
        return threadEnforcer;
    }

    private void checkThreadAffinity(Event event) {
        Predicate<Thread> enforcer = this.threadEnforcer;
        if (enforcer != null && !enforcer.test(Thread.currentThread())) {
            throw new IllegalStateException("Event " + (event != null ? event.getClass().getName() : "null")
                    + " dispatched on unauthorized thread: " + Thread.currentThread().getName());
        }
    }

    public boolean isDeadEventsEnabled() {
        return deadEventsEnabled;
    }

    public boolean isClosed() {
        return closed;
    }

    /**
     * Fails fast with {@link IllegalStateException} when this bus is closed.
     * Every behavior-producing public entry point (dispatch, register, ...)
     * calls this first so a closed bus never silently accepts new work.
     * Cleanup and query operations (clear, unregister, metrics, ...) stay
     * available on a closed bus and do not call this.
     */
    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException(
                    "EventManager '" + name + "' is closed");
        }
    }

    void setDeadEventsEnabled(boolean deadEventsEnabled) {
        this.deadEventsEnabled = deadEventsEnabled;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Returns a lock-free snapshot of dispatch counters and latency profile. */
    public EventMetrics metrics() {
        return new EventMetrics(null, dispatchedEvents.sum(), handlerInvocations.sum(),
                failures.sum(), totalDurationNanos.sum(), maxDurationNanos.get());
    }

    /** Returns counters for one runtime event type without retaining its class loader forever. */
    public EventMetrics metrics(Class<? extends Event> eventType) {
        if (eventType == null) {
            return new EventMetrics(null, 0L, 0L, 0L, 0L, 0L);
        }
        MetricCounter counter = metricsByType.get(eventType);
        return counter == null
                ? new EventMetrics(eventType, 0L, 0L, 0L, 0L, 0L)
                : counter.snapshot(eventType);
    }

    /** Resets dispatch counters without changing registrations. */
    public void resetMetrics() {
        dispatchedEvents.reset();
        handlerInvocations.reset();
        failures.reset();
        totalDurationNanos.reset();
        maxDurationNanos.set(0L);
        metricsByType.clear();
    }

    /** Clears all registrations, closes child buses, and detaches from parent. Safe to call repeatedly. */
    @Override
    public void close() {
        closed = true;
        if (parent != null) {
            parent.children.remove(this);
        }
        for (EventManager child : new ArrayList<EventManager>(children)) {
            child.close();
        }
        children.clear();
        clear();
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
            return NO_HANDLERS;
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
        return register(eventType, DEFAULT_PRIORITY, false, action);
    }

    public <T extends Event> Subscription register(Class<T> eventType, int priority, Consumer<? super T> action) {
        return register(eventType, priority, false, action);
    }

    public <T extends Event> Subscription registerOnce(Class<T> eventType, Consumer<? super T> action) {
        return registerOnce(eventType, DEFAULT_PRIORITY, false, action);
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
        return registerFiltered(eventType, DEFAULT_PRIORITY, false, filter, action);
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
                DEFAULT_PRIORITY,
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
        return register(DEFAULT_PRIORITY, false, action, eventTypes);
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
    private void maybePurgeDeadHandlers() {
        if (hasWeakHandlers && (dispatchesSinceWeakPurge.incrementAndGet() & 1023) == 0) {
            purgeDeadHandlers();
        }
    }

    /**
     * Clears all registrations and derived state: handlers, dispatch caches,
     * mutation stamps, sticky events, upcasters, interceptors and metrics.
     * Configuration (error handler, error policy, thread affinity, parent
     * link) is left untouched. Safe to call repeatedly.
     */
    public void clear() {
        boolean hadRegistrations = !eventHandlers.isEmpty() || !dispatchCache.isEmpty();
        for (Handler[] handlers : eventHandlers.values()) {
            for (Handler handler : handlers) {
                handler.active.set(false);
            }
        }
        eventHandlers.clear();
        dispatchCache.clear();
        typeMutationStamps.clear();
        upcasters.clear();
        hasUpcasters = false;
        stickyEvents.clear();
        interceptorList.clear();
        interceptor = null;
        hasWeakHandlers = false;
        dispatchesSinceWeakPurge.set(0);
        resetMetrics();
        if (hadRegistrations) {
            mutationVersion.incrementAndGet();
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

    /**
     * Dispatches an event to all matching handlers and returns it.
     *
     * <p>Post-dispatch precedence: when upcasters are registered for the event's
     * type, the converted events are dispatched afterwards, and the original
     * event is then considered handled — it neither bubbles to the parent bus
     * nor produces a {@link DeadEvent}. Otherwise the event bubbles to the
     * parent bus (unless cancelled or stopped), and only if no parent and no
     * handler exists may a {@link DeadEvent} be emitted.
     *
     * @param event the event to dispatch
     * @param <T>   the event type
     * @return the dispatched event, for chaining
     */
    public <T extends Event> T dispatch(T event) {
        if (event == null) {
            return null;
        }
        ensureOpen();

        EventTransactions.EventTransaction tx = EventTransactions.current();
        if (tx != null) {
            tx.buffer.add(new EventTransactions.BufferedEvent(event, false));
            return event;
        }

        checkThreadAffinity(event);
        maybePurgeDeadHandlers();

        long startNanos = metricsEnabled ? System.nanoTime() : 0L;
        if (metricsEnabled) {
            dispatchedEvents.increment();
            recordDispatch(event.getClass());
        }

        List<EventUpcaster.Typed<?, ?>> upcasterList = hasUpcasters ? upcasters.get(event.getClass()) : null;
        Handler[] handlers = handlersFor(event.getClass());
        if (handlers.length == 0) {
            if (this.interceptor != null) {
                dispatch(event, NO_HANDLERS);
            }
            if (metricsEnabled) {
                recordDuration(event.getClass(), System.nanoTime() - startNanos);
            }
            applyUpcasters(event, upcasterList);
            if (canBubbleToParent(event)) {
                // Fail-fast: if the parent was closed concurrently, its
                // dispatch throws IllegalStateException and the failure
                // propagates to this dispatch's caller.
                parent.dispatch(event);
            } else if (parent == null && deadEventsEnabled && !(event instanceof DeadEvent) && hasListeners(DeadEvent.class)) {
                dispatch(new DeadEvent(this, event, EventTrace.capture()));
            }
            return event;
        }

        try {
            dispatch(event, handlers);
        } finally {
            if (metricsEnabled) {
                recordDuration(event.getClass(), System.nanoTime() - startNanos);
            }
        }

        applyUpcasters(event, upcasterList);
        if (canBubbleToParent(event)) {
            parent.dispatch(event);
        }

        return event;
    }

    /**
     * Applies the registered upcasters for the event's runtime type, dispatching
     * each converted event. Extracted so the empty-handler and normal dispatch
     * paths share identical upcaster semantics.
     */
    private void applyUpcasters(Event event, List<EventUpcaster.Typed<?, ?>> upcasterList) {
        if (upcasterList == null || upcasterList.isEmpty()) {
            return;
        }
        for (EventUpcaster.Typed<?, ?> upcaster : upcasterList) {
            @SuppressWarnings("unchecked")
            EventUpcaster.Typed<Object, Object> typed = (EventUpcaster.Typed<Object, Object>) upcaster;
            Object upcasted = typed.upcast(event);
            if (upcasted instanceof Event) {
                dispatch((Event) upcasted);
            }
        }
    }

    /**
     * Returns whether the event may bubble to the parent bus: a parent must
     * exist and the event must not have been cancelled or stopped.
     */
    private boolean canBubbleToParent(Event event) {
        if (parent == null) {
            return false;
        }
        if (event instanceof Cancellable && ((Cancellable) event).isCancelled()) {
            return false;
        }
        if (event instanceof Stoppable && ((Stoppable) event).isStopped()) {
            return false;
        }
        return true;
    }

    public <T extends Event> T dispatch(T event, Runnable afterDispatch) {
        if (event == null) {
            return null;
        }
        try {
            return dispatch(event);
        } finally {
            if (afterDispatch != null) {
                afterDispatch.run();
            }
        }
    }

    public <T extends Event> T dispatchExact(T event) {
        if (event == null) {
            return null;
        }
        ensureOpen();

        EventTransactions.EventTransaction tx = EventTransactions.current();
        if (tx != null) {
            tx.buffer.add(new EventTransactions.BufferedEvent(event, true));
            return event;
        }

        checkThreadAffinity(event);
        maybePurgeDeadHandlers();

        long startNanos = metricsEnabled ? System.nanoTime() : 0L;
        if (metricsEnabled) {
            dispatchedEvents.increment();
            recordDispatch(event.getClass());
        }

        Handler[] handlers = exactHandlersFor(event.getClass());
        if (handlers.length != 0) {
            try {
                dispatch(event, handlers);
            } finally {
                if (metricsEnabled) {
                    recordDuration(event.getClass(), System.nanoTime() - startNanos);
                }
            }
        } else {
            if (this.interceptor != null) {
                dispatch(event, NO_HANDLERS);
            }
            if (metricsEnabled) {
                recordDuration(event.getClass(), System.nanoTime() - startNanos);
            }
            if (parent != null) {
                // Fail-fast like dispatch(): a concurrently closed parent
                // throws IllegalStateException here.
                parent.dispatchExact(event);
            } else if (deadEventsEnabled && !(event instanceof DeadEvent) && hasListeners(DeadEvent.class)) {
                dispatch(new DeadEvent(this, event, EventTrace.capture()));
            }
        }
        return event;
    }

    public <T extends Event> T dispatchExact(T event, Runnable afterDispatch) {
        if (event == null) {
            return null;
        }
        try {
            return dispatchExact(event);
        } finally {
            if (afterDispatch != null) {
                afterDispatch.run();
            }
        }
    }

    /**
     * Dispatches a cancellable event and returns whether it was cancelled.
     */
    public <T extends Event & Cancellable> boolean dispatchCancelled(T event) {
        if (event == null) {
            return false;
        }
        dispatch(event);
        return event.isCancelled();
    }

    /**
     * Dispatches multiple events in sequential order.
     */
    public void dispatchAll(Event... events) {
        if (events == null || events.length == 0) {
            return;
        }
        for (int i = 0; i < events.length; i++) {
            Event event = events[i];
            if (event != null) {
                dispatch(event);
            }
        }
    }

    /**
     * Dispatches an iterable collection of events in sequential order.
     */
    public void dispatchAll(Iterable<? extends Event> events) {
        if (events == null) {
            return;
        }
        for (Event event : events) {
            if (event != null) {
                dispatch(event);
            }
        }
    }

    /**
     * Dispatches an event asynchronously using ForkJoinPool.commonPool().
     */
    public <T extends Event> CompletableFuture<T> dispatchAsync(final T event) {
        return dispatchAsync(event, this.defaultExecutor);
    }

    /**
     * Dispatches an event through the supplied executor. The returned future
     * completes with the same event instance after dispatch finishes.
     */
    public <T extends Event> CompletableFuture<T> dispatchAsync(final T event, Executor executor) {
        return submit(event, executor, false);
    }

    /**
     * Dispatches an event to exact-type handlers asynchronously using ForkJoinPool.commonPool().
     */
    public <T extends Event> CompletableFuture<T> dispatchExactAsync(final T event) {
        return dispatchExactAsync(event, this.defaultExecutor);
    }

    /**
     * Dispatches an event to exact-type handlers through the supplied executor.
     */
    public <T extends Event> CompletableFuture<T> dispatchExactAsync(final T event, Executor executor) {
        return submit(event, executor, true);
    }

    private <T extends Event> CompletableFuture<T> submit(final T event, Executor executor,
                                                            final boolean exact) {
        final CompletableFuture<T> future = new CompletableFuture<T>();
        if (event == null) {
            future.complete(null);
            return future;
        }
        if (executor == null) {
            future.completeExceptionally(new NullPointerException("executor"));
            return future;
        }
        final EventContext callingContext = EventContext.current();
        try {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    EventContext.Scope scope = callingContext != null && callingContext != EventContext.empty()
                            ? callingContext.attach()
                            : null;
                    try {
                        future.complete(exact ? dispatchExact(event) : dispatch(event));
                    } catch (Throwable t) {
                        future.completeExceptionally(t);
                    } finally {
                        if (scope != null) {
                            scope.close();
                        }
                    }
                }
            });
        } catch (Throwable submissionFailure) {
            future.completeExceptionally(submissionFailure);
        }
        return future;
    }

    public <T extends Event> T dispatch(Class<T> eventType, Supplier<T> supplier) {
        if (eventType == null || supplier == null) {
            return null;
        }
        ensureOpen();
        if (!hasListeners(eventType)) {
            return null;
        }
        return dispatch(supplier.get());
    }

    public <T extends Event> T dispatchExact(Class<T> eventType, Supplier<T> supplier) {
        if (eventType == null || supplier == null) {
            return null;
        }
        ensureOpen();
        if (!hasExactListeners(eventType)) {
            return null;
        }
        return dispatchExact(supplier.get());
    }

    private static final class DispatchFrame implements Runnable {
        private EventManager bus;
        private Event event;
        private Handler[] handlers;
        private DispatchFrame next;
        private volatile boolean executed;

        private void init(EventManager bus, Event event, Handler[] handlers) {
            this.bus = bus;
            this.event = event;
            this.handlers = handlers;
            this.executed = false;
        }

        private void clear() {
            this.bus = null;
            this.event = null;
            this.handlers = null;
        }

        @Override
        public void run() {
            final EventManager targetBus;
            final Event targetEvent;
            final Handler[] targetHandlers;
            synchronized (this) {
                if (executed) {
                    throw new IllegalStateException("proceed() must be called at most once per dispatch");
                }
                executed = true;
                // Detach under release()'s monitor so a racing release()
                // can't null fields mid-read: async proceed() neither loses
                // the event nor dispatches half-nulled state.
                targetBus = this.bus;
                targetEvent = this.event;
                targetHandlers = this.handlers;
                this.bus = null;
                this.event = null;
                this.handlers = null;
            }
            if (targetBus != null) {
                targetBus.doDispatch(targetEvent, targetHandlers);
            }
        }
    }

    private static final class FramePool {
        private DispatchFrame head = new DispatchFrame();

        private DispatchFrame acquire(EventManager bus, Event event, Handler[] handlers) {
            DispatchFrame frame = head;
            if (frame == null) {
                frame = new DispatchFrame();
            } else {
                head = frame.next;
                frame.next = null;
            }
            frame.init(bus, event, handlers);
            return frame;
        }

        private void release(DispatchFrame frame) {
            synchronized (frame) {
                frame.clear();
                frame.next = head;
                head = frame;
            }
        }
    }

    private static final ThreadLocal<FramePool> FRAME_POOLS = new ThreadLocal<FramePool>() {
        @Override
        protected FramePool initialValue() {
            return new FramePool();
        }
    };

    private void dispatch(final Event event, final Handler[] handlers) {
        EventInterceptor currentInterceptor = this.interceptor;
        if (currentInterceptor != null) {
            FramePool pool = FRAME_POOLS.get();
            DispatchFrame frame = pool.acquire(this, event, handlers);
            try {
                currentInterceptor.intercept(event, frame);
            } finally {
                if (frame.executed) {
                    pool.release(frame);
                }
            }
        } else {
            doDispatch(event, handlers);
        }
    }

    private void doDispatch(Event event, Handler[] handlers) {
        Cancellable cancellable = event instanceof Cancellable ? (Cancellable) event : null;
        Stoppable stoppable = event instanceof Stoppable ? (Stoppable) event : null;
        boolean stoppableReadable = true;
        boolean cancellableReadable = true;
        List<Throwable> aggregatedErrors = (errorPolicy == ErrorPolicy.AGGREGATE) ? new ArrayList<Throwable>() : null;

        for (int i = 0; i < handlers.length; i++) {
            Handler handler = handlers[i];

            // A dispatch snapshot may outlive a concurrent unregister/clear.
            if (!handler.active.get()) {
                continue;
            }

            if (stoppable != null && stoppableReadable) {
                boolean stopped;
                try {
                    stopped = stoppable.isStopped();
                } catch (Throwable t) {
                    if (!handleFailure(event, event, t, aggregatedErrors)) return;
                    // Under CONTINUE or AGGREGATE, treat an unreadable state as false for
                    // this handler instead of repeatedly invoking the broken
                    // accessor for every remaining handler.
                    stopped = false;
                    stoppableReadable = false;
                }
                if (stopped) {
                    break;
                }
            }

            boolean handling;
            try {
                handling = handler.isHandlingEvents(event) && handler.matchesGenericEvent(event);
            } catch (Throwable t) {
                if (!handleFailure(event, handler.listener, t, aggregatedErrors)) return;
                continue;
            }
            if (!handling) {
                continue;
            }

            if (cancellable != null && handler.ignoreCancelled && cancellableReadable) {
                boolean cancelled;
                try {
                    cancelled = cancellable.isCancelled();
                } catch (Throwable t) {
                    if (!handleFailure(event, event, t, aggregatedErrors)) return;
                    // Under CONTINUE or AGGREGATE, treat an unreadable state as false for
                    // this handler; the failure has already been reported.
                    cancelled = false;
                    cancellableReadable = false;
                }
                if (cancelled) {
                    continue;
                }
            }

            if (handler.filter != null) {
                boolean accepted;
                try {
                    accepted = handler.filter.test(event);
                } catch (Throwable t) {
                    if (!handleFailure(event, handler.listener, t, aggregatedErrors)) return;
                    continue;
                }
                if (!accepted) {
                    continue;
                }
            }

            try {
                if (handler.once) {
                    // Claim the single execution right atomically: concurrent
                    // dispatches sharing this snapshot must not double-fire.
                    if (!handler.active.compareAndSet(true, false)) {
                        continue;
                    }
                    removeHandlers(handler.eventType, new Predicate<Handler>() {
                        @Override
                        public boolean test(Handler candidate) {
                            return candidate == handler;
                        }
                    });
                }
                if (metricsEnabled) {
                    handlerInvocations.increment();
                    recordInvocation(event.getClass());
                }
                handler.invoke(event);
            } catch (Throwable t) {
                if (!handleFailure(event, handler.listener, t, aggregatedErrors)) return;
            }
        }

        if (aggregatedErrors != null && !aggregatedErrors.isEmpty()) {
            throw buildAggregatedException(event, aggregatedErrors);
        }
    }

    private boolean handleFailure(Event event, Object source, Throwable throwable) {
        return handleFailure(event, source, throwable, null);
    }

    private boolean handleFailure(Event event, Object source, Throwable throwable, List<Throwable> aggregatedErrors) {
        if (metricsEnabled) {
            failures.increment();
            recordFailure(event == null ? null : event.getClass());
        }
        try {
            errorHandler.handle(event, source, throwable);
        } catch (Throwable errorHandlerFailure) {
            log.log(Level.SEVERE, "Event error handler threw while handling a listener failure",
                    errorHandlerFailure);
        }
        ErrorPolicy policy = errorPolicy;
        if (policy == ErrorPolicy.PROPAGATE) {
            throw propagate(event, source, throwable);
        }
        if (policy == ErrorPolicy.AGGREGATE && aggregatedErrors != null) {
            aggregatedErrors.add(throwable);
            return true;
        }
        return policy == ErrorPolicy.CONTINUE;
    }

    /**
     * Reports a {@link Throwable} that escaped {@link #dispatch} on a background
     * worker thread (for example {@link AsyncEventChannel}'s dispatcher) to the
     * configured {@link EventErrorHandler}. The error policy is deliberately
     * <em>not</em> applied here: propagating would kill the worker thread, so
     * the worker always continues with the next event. This method never throws.
     *
     * <p>Note: listener failures are already reported to the error handler by
     * dispatch itself, so under {@link ErrorPolicy#PROPAGATE} or
     * {@link ErrorPolicy#AGGREGATE} the same failure may be reported twice —
     * once per listener, once at this worker boundary with a {@code null}
     * listener. Failures that dispatch never saw (such as a throwing
     * interceptor) are reported exactly once here.
     */
    void reportAsyncFailure(Event event, Throwable throwable) {
        try {
            errorHandler.handle(event, null, throwable);
        } catch (Throwable errorHandlerFailure) {
            log.log(Level.SEVERE, "Event error handler threw while handling an async dispatch failure",
                    errorHandlerFailure);
        }
    }

    private EventDispatchException buildAggregatedException(Event event, List<Throwable> errors) {
        Throwable primary = errors.get(0);
        String eventName = event == null ? "null" : event.getClass().getName();
        EventDispatchException aggregated = new EventDispatchException(
                "Dispatch of " + eventName + " failed with " + errors.size() + " exception(s)",
                event,
                primary
        );
        for (int i = 1; i < errors.size(); i++) {
            aggregated.addSuppressed(errors.get(i));
        }
        return aggregated;
    }

    private MetricCounter counterFor(Class<?> eventType) {
        if (eventType == null) {
            return null;
        }
        // The check-then-act sequence must be atomic: the synchronized map
        // wrapper alone does not make putIfAbsent atomic, so two racing
        // threads could otherwise create and publish two counters for the
        // same type and lose increments.
        synchronized (metricsByType) {
            MetricCounter counter = metricsByType.get(eventType);
            if (counter == null) {
                counter = new MetricCounter();
                metricsByType.put(eventType, counter);
            }
            return counter;
        }
    }

    private void recordDispatch(Class<?> eventType) {
        MetricCounter counter = counterFor(eventType);
        if (counter != null) counter.dispatchedEvents.increment();
    }

    private void recordInvocation(Class<?> eventType) {
        MetricCounter counter = counterFor(eventType);
        if (counter != null) counter.handlerInvocations.increment();
    }

    private void recordFailure(Class<?> eventType) {
        MetricCounter counter = counterFor(eventType);
        if (counter != null) counter.failures.increment();
    }

    private void recordDuration(Class<?> eventType, long nanos) {
        totalDurationNanos.add(nanos);
        long currentMax;
        while (nanos > (currentMax = maxDurationNanos.get())) {
            if (maxDurationNanos.compareAndSet(currentMax, nanos)) {
                break;
            }
        }
        MetricCounter counter = counterFor(eventType);
        if (counter != null) {
            counter.recordDuration(nanos);
        }
    }

    private static RuntimeException propagate(Event event, Object source, Throwable throwable) {
        if (throwable instanceof RuntimeException) {
            return (RuntimeException) throwable;
        }
        if (throwable instanceof Error) {
            throw (Error) throwable;
        }
        return new EventDispatchException(event, source, throwable);
    }

    private Handler[] handlersFor(Class<?> eventClass) {
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

    private Handler[] exactHandlersFor(Class<?> eventClass) {
        Handler[] handlers = eventHandlers.get(eventClass);
        return handlers == null ? NO_HANDLERS : handlers;
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
            return NO_HANDLERS;
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
            return NO_HANDLERS;
        }
        if (matchingTypes == 1) {
            Handler[] handlers = eventHandlers.get(singleType);
            return (handlers == null || handlers.length == 0) ? NO_HANDLERS : handlers;
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
            return NO_HANDLERS;
        }

        List<Handler> handlers = new ArrayList<Handler>(collected);
        return HandlerOrdering.sortWithDag(handlers);
    }



    private static int normalizePriority(int priority) {
        return priority == Priority.UNSPECIFIED ? DEFAULT_PRIORITY : priority;
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
                replaySticky(definition.eventType, definition.genericType, handler);
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
            replaySticky(definition.eventType, definition.genericType, handler);
        }
    }

    private void addHandler(final Handler handler) {
        // Single choke point for every public register* variant and the
        // on()/subscribe()/expect() builder paths: registering on a closed
        // bus is a programming error, fail fast.
        ensureOpen();
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

    private void removeHandlers(Class<? extends Event> targetType, final Predicate<Handler> predicate) {
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
            return DEFAULT_PRIORITY;
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
        Handler[] handlers = current == null ? NO_HANDLERS : current;
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
            return NO_HANDLERS;
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

    static final class Handler {
        private final Object listener;
        private final WeakReference<Object> weakListener;
        private final Method method;
        private final Field field;
        private final Object dedupeOwner;
        private final Class<? extends Event> eventType;
        final int priority;
        private final boolean ignoreCancelled;
        final long order;
        private final boolean once;
        private final Predicate<Event> filter;
        private final ListenerIntrospection.Invoker invoker;
        private final EventSubscriber subscriber;
        private final Type genericType;
        final String id;
        final String[] after;
        final String[] before;
        final Class<?>[] afterClasses;
        final Class<?>[] beforeClasses;
        private final AtomicBoolean active = new AtomicBoolean(true);

        private Handler(Object listener, boolean weak, Method method, Field field, Object dedupeOwner,
                        Class<? extends Event> eventType, int priority, boolean ignoreCancelled,
                        long order, boolean once, Predicate<Event> filter, Type genericType, ListenerIntrospection.Invoker invoker) {
            this(listener, weak, method, field, dedupeOwner, eventType, priority, ignoreCancelled,
                 order, once, filter, genericType, "", null, null, null, null, invoker);
        }

        private Handler(Object listener, boolean weak, Method method, Field field, Object dedupeOwner,
                        Class<? extends Event> eventType, int priority, boolean ignoreCancelled,
                        long order, boolean once, Predicate<Event> filter, Type genericType,
                        String id, String[] after, String[] before, Class<?>[] afterClasses, Class<?>[] beforeClasses,
                        ListenerIntrospection.Invoker invoker) {
            if (weak && listener != null && !(listener instanceof Class<?>)) {
                this.listener = null;
                this.weakListener = new WeakReference<Object>(listener);
            } else {
                this.listener = listener;
                this.weakListener = null;
            }
            this.method = method;
            this.field = field;
            this.dedupeOwner = dedupeOwner;
            this.eventType = eventType;
            this.priority = priority;
            this.ignoreCancelled = ignoreCancelled;
            this.order = order;
            this.once = once;
            this.filter = filter;
            this.genericType = genericType;
            this.id = id != null ? id : "";
            this.after = after;
            this.before = before;
            this.afterClasses = afterClasses;
            this.beforeClasses = beforeClasses;
            this.invoker = invoker;
            this.subscriber = (listener instanceof EventSubscriber && !weak) ? (EventSubscriber) listener : null;
        }

        Class<?> getListenerClass() {
            if (dedupeOwner instanceof Class<?>) {
                return (Class<?>) dedupeOwner;
            }
            if (listener != null) {
                return listener.getClass();
            }
            if (weakListener != null) {
                Object ref = weakListener.get();
                if (ref != null) return ref.getClass();
            }
            if (method != null) {
                return method.getDeclaringClass();
            }
            if (field != null) {
                return field.getDeclaringClass();
            }
            return null;
        }

        private Handler(Object listener, boolean weak, Method method, Field field, Object dedupeOwner,
                        Class<? extends Event> eventType, int priority, boolean ignoreCancelled,
                        long order, boolean once, Predicate<Event> filter, ListenerIntrospection.Invoker invoker) {
            this(listener, weak, method, field, dedupeOwner, eventType, priority, ignoreCancelled, order, once, filter, null, invoker);
        }

        private Handler(Object listener, Method method, Field field, Object dedupeOwner,
                        Class<? extends Event> eventType, int priority, boolean ignoreCancelled,
                        long order, boolean once, Predicate<Event> filter, ListenerIntrospection.Invoker invoker) {
            this(listener, false, method, field, dedupeOwner, eventType, priority, ignoreCancelled, order, once, filter, invoker);
        }

        private boolean isWeak() {
            return weakListener != null;
        }

        private boolean isDead() {
            return weakListener != null && weakListener.get() == null;
        }

        private Object getListener() {
            return weakListener != null ? weakListener.get() : listener;
        }

        private boolean matchesGenericEvent(Event event) {
            if (genericType == null || !(event instanceof GenericEvent<?>)) {
                return true;
            }
            Type actualType = ((GenericEvent<?>) event).getGenericType();
            return ListenerIntrospection.isGenericTypeAssignable(genericType, actualType);
        }

        private boolean matchesListener(Object candidate) {
            if (candidate == null) return false;
            if (listener == candidate) return true;
            if (weakListener != null) {
                Object target = weakListener.get();
                return target == candidate;
            }
            return false;
        }

        private void invoke(Event event) throws Throwable {
            invoker.invoke(event);
        }

        private boolean isHandlingEvents(Event event) {
            if (weakListener != null) {
                Object target = weakListener.get();
                if (target == null) {
                    active.set(false);
                    return false;
                }
                if (target instanceof EventSubscriber) {
                    return ((EventSubscriber) target).isHandlingEvents(event);
                }
                return true;
            }
            return subscriber == null || subscriber.isHandlingEvents(event);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof Handler)) {
                return false;
            }
            Handler other = (Handler) obj;
            if (method == null && field == null) {
                return false;
            }
            if (method != null || other.method != null) {
                return method != null
                        && other.method != null
                        && dedupeOwner == other.dedupeOwner
                        && method.equals(other.method);
            }
            if (field == null || other.field == null) {
                return false;
            }
            return dedupeOwner == other.dedupeOwner && field.equals(other.field);
        }

        @Override
        public int hashCode() {
            Object member = method != null ? method : field;
            if (member == null) {
                return System.identityHashCode(this);
            }
            return 31 * System.identityHashCode(dedupeOwner) + Objects.hashCode(member);
        }
    }

    private static final class MetricCounter {
        private final LongAdder dispatchedEvents = new LongAdder();
        private final LongAdder handlerInvocations = new LongAdder();
        private final LongAdder failures = new LongAdder();
        private final LongAdder totalDurationNanos = new LongAdder();
        private final AtomicLong maxDurationNanos = new AtomicLong();

        private void recordDuration(long nanos) {
            totalDurationNanos.add(nanos);
            long currentMax;
            while (nanos > (currentMax = maxDurationNanos.get())) {
                if (maxDurationNanos.compareAndSet(currentMax, nanos)) {
                    break;
                }
            }
        }

        private EventMetrics snapshot(Class<?> eventType) {
            return new EventMetrics(eventType, dispatchedEvents.sum(),
                    handlerInvocations.sum(), failures.sum(),
                    totalDurationNanos.sum(), maxDurationNanos.get());
        }
    }


    /**
     * Fluent builder for creating configured {@link EventManager} instances.
     */
    @Override
    public String toString() {
        return "EventManager[name=\"" + name + "\""
                + ", registeredTypes=" + eventHandlers.size()
                + ", handlers=" + handlerCount()
                + ", metrics=" + metricsEnabled
                + ", closed=" + closed
                + (parent != null ? ", parent=\"" + parent.getName() + "\"" : "")
                + ']';
    }


    // ==========================================
    // Transactional Event Buffering & Rollback
    // ==========================================

    /**
     * Executes the given action within an event transaction.
     * <p>
     * Any events dispatched on the calling thread during the transaction are buffered.
     * If the action completes normally, all buffered events are committed and flushed
     * sequentially to listeners. If the action throws an exception or the transaction is
     * rolled back, all buffered events are discarded.
     *
     * @param action the transactional block
     */
    public void transaction(Runnable action) {
        ensureOpen();
        EventTransactions.execute(this, action);
    }

    /**
     * Executes the given supplier within an event transaction and returns its result.
     *
     * @param action the transactional block producing a result
     * @param <R>    the return type
     * @return the result of the action
     */
    public <R> R transaction(Supplier<R> action) {
        ensureOpen();
        return EventTransactions.execute(this, action);
    }

    /**
     * Returns whether an event transaction is currently active on the calling thread.
     */
    /**
     * Executes an event transaction with access to the {@link TransactionContext} for Saga compensations.
     *
     * @param action transactional logic
     */
    public void transaction(final Consumer<TransactionContext> action) {
        ensureOpen();
        EventTransactions.executeWithContext(this, action);
    }

    /**
     * Retrieves the active {@link TransactionContext} for the calling thread, or {@code null} if none.
     */
    public TransactionContext getTransactionContext() {
        return EventTransactions.contextOrNull();
    }

    /**
     * Exports the listener subscription, DAG dependencies, and upcaster topology as a GitHub Mermaid graph.
     */
    public String exportTopology() {
        return exportTopology(TopologyFormat.MERMAID);
    }

    /**
     * Exports the event bus topology in the specified {@link TopologyFormat}.
     */
    public String exportTopology(TopologyFormat format) {
        return TopologyExporter.export(this, format != null ? format : TopologyFormat.MERMAID);
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

    Map<Class<?>, List<Class<?>>> getUpcasterTopologySnapshot() {
        Map<Class<?>, List<Class<?>>> result = new java.util.LinkedHashMap<Class<?>, List<Class<?>>>();
        for (Map.Entry<Class<?>, List<EventUpcaster.Typed<?, ?>>> entry : upcasters.entrySet()) {
            List<Class<?>> targets = new ArrayList<Class<?>>();
            for (EventUpcaster.Typed<?, ?> u : entry.getValue()) {
                targets.add(u.getTargetType());
            }
            result.put(entry.getKey(), targets);
        }
        return result;
    }

    /**
     * Starts a non-invasive event recorder capturing all event dispatches and relative timings.
     *
     * @return the active EventRecorder
     */
    public EventRecorder startRecording() {
        return startRecording(null);
    }

    /**
     * Starts an event recorder with an event filter predicate.
     *
     * @param filter predicate selecting events to record
     * @return the active EventRecorder
     */
    public EventRecorder startRecording(Predicate<Event> filter) {
        return new EventRecorder(this, filter);
    }

    public boolean isTransactionActive() {
        return EventTransactions.current() != null;
    }

    /**
     * Marks the active event transaction on the calling thread as rolled back.
     * All events buffered in this transaction will be discarded.
     */
    public void rollbackTransaction() {
        EventTransactions.EventTransaction tx = EventTransactions.current();
        if (tx != null) {
            tx.rolledBack = true;
        }
    }

    // ==========================================
    // Fluent Subscriber Builder DSL
    // ==========================================

    /**
     * Starts a fluent subscription builder for the specified event type.
     *
     * @param eventType the event class
     * @param <T>       the event type
     * @return a fluent builder to configure priority, filter, once, weak, etc.
     */
    public <T extends Event> SubscriberBuilder<T> on(Class<T> eventType) {
        return new SubscriberBuilder<T>(this, eventType);
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
            replaySticky(eventType, genericType, handler);
        }
        return new HandlerSubscription(handler);
    }

    // ==========================================
    // Standard Dispatch API Aliases
    // ==========================================

    static ScheduledExecutorService getTimeoutScheduler() {
        return TimeoutScheduler.scheduler();
    }

    // ==========================================
    // Asynchronous Event Expectation & Future Await
    // ==========================================

    /**
     * Returns a {@link CompletableFuture} that completes when the next event of the specified
     * type is dispatched. The internal listener unregisters automatically once completed.
     */
    public <T extends Event> CompletableFuture<T> expect(Class<T> eventType) {
        return expect(eventType, null, 0L, null);
    }

    /**
     * Returns a {@link CompletableFuture} that completes when the next event of the specified
     * type matching the filter predicate is dispatched.
     */
    public <T extends Event> CompletableFuture<T> expect(Class<T> eventType, Predicate<? super T> filter) {
        return expect(eventType, filter, 0L, null);
    }

    /**
     * Returns a {@link CompletableFuture} that completes when the next matching event is dispatched,
     * or completes exceptionally with {@link TimeoutException} if the timeout expires first.
     *
     * @param eventType the event class to await
     * @param filter    optional filter predicate (may be {@code null})
     * @param timeout   maximum time to wait
     * @param unit      unit of timeout duration
     */
    public <T extends Event> CompletableFuture<T> expect(
            final Class<T> eventType,
            final Predicate<? super T> filter,
            final long timeout,
            final TimeUnit unit) {
        return EventExpectations.await(this, eventType, filter, timeout, unit);
    }

    // ==========================================
    // Reactive Event Publisher
    // ==========================================

    /**
     * Creates a reactive {@link EventPublisher} for the specified event class, enabling
     * functional stream subscription and filtering.
     *
     * @param eventType the event type to stream
     * @param <T>       the event type parameter
     * @return an EventPublisher for this event type
     */
    public <T extends Event> EventPublisher<T> asPublisher(final Class<T> eventType) {
        Objects.requireNonNull(eventType, "eventType");
        final EventManager self = this;
        return new EventPublisher<T>() {
            @Override
            public Class<T> getEventType() {
                return eventType;
            }

            @Override
            public Subscription subscribe(Consumer<? super T> subscriber) {
                return subscribe(null, subscriber);
            }

            @Override
            public Subscription subscribe(Predicate<? super T> filter, Consumer<? super T> subscriber) {
                return self.on(eventType).filter(filter).handle(subscriber);
            }
        };
    }

    // ==========================================
    // Context-Aware Event Dispatching
    // ==========================================

    /**
     * Dispatches an event within the scope of the specified {@link EventContext}.
     * The context is bound to the thread during dispatch and can be retrieved
     * by listeners via {@link EventContext#current()}.
     *
     * @param event   the event to dispatch
     * @param context the metadata context
     * @param <T>     the event type
     * @return the dispatched event
     */
    public <T extends Event> T dispatch(T event, EventContext context) {
        if (context == null || context == EventContext.empty()) {
            return dispatch(event);
        }
        EventContext.Scope scope = context.attach();
        try {
            return dispatch(event);
        } finally {
            scope.close();
        }
    }

    /**
     * Dispatches an event to exact handlers within the scope of the specified {@link EventContext}.
     */
    public <T extends Event> T dispatchExact(T event, EventContext context) {
        if (context == null || context == EventContext.empty()) {
            return dispatchExact(event);
        }
        EventContext.Scope scope = context.attach();
        try {
            return dispatchExact(event);
        } finally {
            scope.close();
        }
    }


    // ==========================================
    // Sticky Events Support
    // ==========================================

    /**
     * Dispatches an event and caches it as the latest sticky event for its concrete runtime type.
     * Newly registered listeners marked as sticky will immediately receive this cached event upon registration.
     *
     * @param event the event to dispatch and cache
     * @param <T>   the event type
     * @return the dispatched event
     */
    public <T extends Event> T dispatchSticky(T event) {
        if (event == null) {
            return null;
        }
        ensureOpen();
        EventTransactions.EventTransaction tx = EventTransactions.current();
        if (tx != null) {
            // Buffer the sticky write together with the dispatch: on rollback
            // the sticky cache must not retain an event that never happened.
            tx.buffer.add(new EventTransactions.BufferedEvent(event, false, true));
            return event;
        }
        writeSticky(event);
        return dispatch(event);
    }

    /**
     * Writes the sticky cache entry for the event's runtime type (including the
     * upcaster cascade) without dispatching it. Package-private so the
     * transaction machinery can replay it at commit time.
     */
    void writeSticky(Event event) {
        stickyEvents.put(event.getClass(), event);
        cascadeStickyUpcasting(event);
    }

    private void cascadeStickyUpcasting(Event event) {
        List<EventUpcaster.Typed<?, ?>> upcasterList = hasUpcasters ? upcasters.get(event.getClass()) : null;
        if (upcasterList != null && !upcasterList.isEmpty()) {
            for (EventUpcaster.Typed<?, ?> upcaster : upcasterList) {
                @SuppressWarnings("unchecked")
                EventUpcaster.Typed<Object, Object> typed = (EventUpcaster.Typed<Object, Object>) upcaster;
                Object upcasted = typed.upcast(event);
                if (upcasted instanceof Event) {
                    Event upcastedEvent = (Event) upcasted;
                    stickyEvents.put(upcastedEvent.getClass(), upcastedEvent);
                    cascadeStickyUpcasting(upcastedEvent);
                }
            }
        }
    }

    /**
     * Registers a typed event upcaster for automatic schema version migration.
     *
     * @param upcaster the typed upcaster
     * @param <S> source event type
     * @param <T> target event type
     */
    public <S, T> void registerUpcaster(EventUpcaster.Typed<S, T> upcaster) {
        Objects.requireNonNull(upcaster, "upcaster");
        ensureOpen();
        Class<?> source = upcaster.getSourceType();
        Class<?> target = upcaster.getTargetType();
        if (source != null && target != null && reachesUpcasterTarget(target, source)) {
            throw new IllegalArgumentException("Registering upcaster " + source.getName()
                    + " -> " + target.getName() + " would create an upcaster cycle");
        }
        List<EventUpcaster.Typed<?, ?>> list = upcasters.get(source);
        if (list == null) {
            list = new CopyOnWriteArrayList<EventUpcaster.Typed<?, ?>>();
            List<EventUpcaster.Typed<?, ?>> existing = upcasters.putIfAbsent(source, list);
            if (existing != null) {
                list = existing;
            }
        }
        list.add(upcaster);
        hasUpcasters = true;
    }

    /**
     * Returns whether {@code from} can reach {@code to} by following registered
     * upcaster edges (source -&gt; target). Used to reject registrations that
     * would create a conversion cycle and recurse forever at dispatch time.
     */
    private boolean reachesUpcasterTarget(Class<?> from, Class<?> to) {
        Set<Class<?>> visited = new HashSet<Class<?>>();
        Deque<Class<?>> stack = new ArrayDeque<Class<?>>();
        stack.push(from);
        while (!stack.isEmpty()) {
            Class<?> current = stack.pop();
            if (current.equals(to)) {
                return true;
            }
            if (!visited.add(current)) {
                continue;
            }
            List<EventUpcaster.Typed<?, ?>> outgoing = upcasters.get(current);
            if (outgoing != null) {
                for (EventUpcaster.Typed<?, ?> u : outgoing) {
                    Class<?> next = u.getTargetType();
                    if (next != null) {
                        stack.push(next);
                    }
                }
            }
        }
        return false;
    }

    /**
     * Registers an event upcaster function from source class to target class.
     *
     * @param source source class
     * @param target target class
     * @param mapper mapping function
     * @param <S> source type
     * @param <T> target type
     */
    public <S, T> void registerUpcaster(Class<S> source, Class<T> target, java.util.function.Function<S, T> mapper) {
        registerUpcaster(EventUpcaster.of(source, target, mapper));
    }

    /**
     * Creates a high-performance bounded asynchronous event channel with backpressure control.
     *
     * @param eventType the event class
     * @param capacity the ring buffer / queue capacity
     * @param policy the backpressure overflow strategy
     * @param <T> event type
     * @return the created AsyncEventChannel
     */
    public <T extends Event> AsyncEventChannel<T> createChannel(Class<T> eventType, int capacity, BackpressurePolicy policy) {
        return new AsyncEventChannel<T>(this, eventType, capacity, policy);
    }

    /**
     * Retrieves the latest cached sticky event of the specified class, or {@code null} if none exists.
     *
     * @param eventType the event class
     * @param <T>       the event type
     * @return the cached sticky event instance, or {@code null}
     */
    public <T extends Event> T getSticky(Class<T> eventType) {
        if (eventType == null) {
            return null;
        }
        Event event = stickyEvents.get(eventType);
        if (event == null) {
            for (Map.Entry<Class<? extends Event>, Event> entry : stickyEvents.entrySet()) {
                if (eventType.isAssignableFrom(entry.getKey())) {
                    return eventType.cast(entry.getValue());
                }
            }
        }
        return eventType.cast(event);
    }

    /**
     * Removes and returns the cached sticky event of the specified class.
     *
     * @param eventType the event class
     * @param <T>       the event type
     * @return the removed event, or {@code null}
     */
    public <T extends Event> T removeSticky(Class<T> eventType) {
        if (eventType == null) {
            return null;
        }
        Event event = stickyEvents.remove(eventType);
        return eventType.cast(event);
    }

    /**
     * Clears all cached sticky events from this event bus.
     */
    public void clearSticky() {
        stickyEvents.clear();
    }

    void replaySticky(Class<?> targetType, Type genericType, Handler handler) {
        if (stickyEvents.isEmpty()) {
            return;
        }
        for (Event sticky : stickyEvents.values()) {
            if (targetType.isInstance(sticky)) {
                try {
                    if (handler.isHandlingEvents(sticky) && handler.matchesGenericEvent(sticky)) {
                        handler.invoke(sticky);
                    }
                } catch (Throwable t) {
                    handleFailure(sticky, handler.listener, t);
                }
            }
        }
    }

    // ==========================================
    // Flow.Publisher Reactive Streams Adapter
    // ==========================================

    /**
     * Adapts this event bus to a JDK 9+ {@code java.util.concurrent.Flow.Publisher<T>}.
     *
     * @param eventType the event class to publish
     * @param <T>       the event type
     * @return an instance of {@code java.util.concurrent.Flow.Publisher<T>}
     * @throws UnsupportedOperationException if running on JDK 8
     */
    public <T extends Event> Object asFlowPublisher(Class<T> eventType) {
        return FlowPublisherAdapter.asFlowPublisher(this, eventType);
    }

    public static final class Builder {
        private String name = "EventManager";
        private Executor defaultExecutor;
        private ErrorPolicy errorPolicy = ErrorPolicy.CONTINUE;
        private EventErrorHandler errorHandler = DEFAULT_ERROR_HANDLER;
        private boolean metricsEnabled = true;
        private boolean deadEventsEnabled = true;
        private boolean useVirtualThreads = false;
        private final List<EventInterceptor> interceptors = new ArrayList<EventInterceptor>();
        private Predicate<Thread> threadEnforcer;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder defaultExecutor(Executor defaultExecutor) {
            this.defaultExecutor = defaultExecutor;
            return this;
        }

        /**
         * Enables automatic Project Loom virtual thread per-task execution for async dispatches
         * if running on JDK 21+, gracefully falling back to standard platform thread pools on JDK 8-20.
         */
        public Builder useVirtualThreads() {
            return useVirtualThreads(true);
        }

        /**
         * Configures whether to enable Project Loom virtual threads when available.
         */
        public Builder useVirtualThreads(boolean enable) {
            this.useVirtualThreads = enable;
            return this;
        }

        public Builder errorPolicy(ErrorPolicy errorPolicy) {
            this.errorPolicy = errorPolicy != null ? errorPolicy : ErrorPolicy.CONTINUE;
            return this;
        }

        public Builder errorHandler(EventErrorHandler errorHandler) {
            this.errorHandler = errorHandler != null ? errorHandler : DEFAULT_ERROR_HANDLER;
            return this;
        }

        public Builder metricsEnabled(boolean metricsEnabled) {
            this.metricsEnabled = metricsEnabled;
            return this;
        }

        public Builder deadEventsEnabled(boolean deadEventsEnabled) {
            this.deadEventsEnabled = deadEventsEnabled;
            return this;
        }

        public Builder interceptor(EventInterceptor interceptor) {
            if (interceptor != null) {
                this.interceptors.add(interceptor);
            }
            return this;
        }

        public Builder enforceThread(final Thread thread) {
            if (thread == null) {
                this.threadEnforcer = null;
            } else {
                this.threadEnforcer = new Predicate<Thread>() {
                    @Override
                    public boolean test(Thread candidate) {
                        return candidate == thread;
                    }
                };
            }
            return this;
        }

        public Builder enforceThread(Predicate<Thread> threadEnforcer) {
            this.threadEnforcer = threadEnforcer;
            return this;
        }

        public EventManager build() {
            Executor executor = this.defaultExecutor;
            if (executor == null && this.useVirtualThreads) {
                executor = VirtualThreadSupport.createVirtualThreadExecutor();
            }
            EventManager bus = new EventManager(this.name, null, this.errorHandler, this.errorPolicy, executor);
            bus.setMetricsEnabled(this.metricsEnabled);
            bus.setDeadEventsEnabled(this.deadEventsEnabled);
            if (!this.interceptors.isEmpty()) {
                bus.setInterceptor(EventInterceptors.chain(this.interceptors));
            }
            bus.enforceThread(this.threadEnforcer);
            return bus;
        }
    }
}
