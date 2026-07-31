package com.cubk.event;

import com.cubk.event.annotations.EventPriority;
import com.cubk.event.annotations.EventTarget;
import com.cubk.event.impl.Cancellable;
import com.cubk.event.impl.Event;
import com.cubk.event.impl.Stoppable;

import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A thread-safe event bus supporting annotated handlers, field listeners, and functional listeners.
 *
 * <p>Features:
 * <ul>
 *     <li>Annotation driven handlers via {@link EventTarget} with {@link EventPriority} ordering
 *     (lower value runs first, ties broken by registration order).</li>
 *     <li>Functional registration: {@link #register(Class, Consumer)} returns a {@link Subscription}.</li>
 *     <li>{@link EventListener} instances can be registered directly or as annotated fields, and can
 *     provide their own default priority.</li>
 *     <li>Static-only registration of utility classes via {@link #register(Class)} - no instance needed.</li>
 *     <li>Handlers are discovered across the listener's whole class hierarchy (superclasses and interfaces).</li>
 *     <li>Listener class scan plans and method invoker factories are cached for cheap repeated registration.</li>
 *     <li>Events are dispatched to handlers registered for any supertype of the event
 *     (superclasses and interfaces), with optional exact-type dispatch and cached flattened arrays.</li>
 *     <li>Static and private handler methods are supported.</li>
 *     <li>Invocation goes through {@link LambdaMetafactory} when possible (using a private lookup on
 *     Java 9+ so even private handlers take the fast path), falling back to {@link MethodHandle} and
 *     finally plain reflection. Invoker factories are cached per method, so repeatedly registering and
 *     unregistering the same listener class or component is cheap.</li>
 *     <li>{@link Cancellable} events carry an application-defined cancellation result and can skip
 *     handlers marked {@code ignoreCancelled}; {@link Stoppable} events abort dispatch entirely.</li>
 *     <li>{@link EventSubscriber} listeners can temporarily opt out without unregistering.</li>
 *     <li>Lazy dispatch via {@link #call(Class, Supplier)} avoids constructing events nobody listens to.</li>
 *     <li>Handler exceptions are isolated and routed to a pluggable {@link EventErrorHandler}.</li>
 * </ul>
 */
public class EventManager {
    private static final Logger LOGGER = Logger.getLogger(EventManager.class.getName());
    private static final int DEFAULT_PRIORITY = Priority.NORMAL;
    private static final Handler[] NO_HANDLERS = new Handler[0];
    private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();
    // MethodHandles.privateLookupIn(Class, Lookup) only exists on Java 9+; resolved reflectively
    // so the library still compiles and runs on Java 8.
    private static final Method PRIVATE_LOOKUP_IN = resolvePrivateLookupIn();
    private static final EventErrorHandler DEFAULT_ERROR_HANDLER = new EventErrorHandler() {
        @Override
        public void handle(Event event, Object listener, Throwable throwable) {
            LOGGER.log(Level.SEVERE, "Failed to dispatch " + event.getClass().getName()
                    + " to listener " + listener, throwable);
        }
    };
    private static final Comparator<Handler> HANDLER_ORDER = new Comparator<Handler>() {
        @Override
        public int compare(Handler left, Handler right) {
            int priorityCompare = Integer.compare(left.priority, right.priority);
            if (priorityCompare != 0) {
                return priorityCompare;
            }
            return Long.compare(left.order, right.order);
        }
    };

    private final Map<Class<? extends Event>, CopyOnWriteArrayList<Handler>> eventHandlers;
    private final Map<Class<?>, CachedDispatch> dispatchCache;
    private final Map<Method, InvokerFactory> invokerFactories;
    private final Map<Class<?>, ListenerPlan> listenerPlans;
    private final AtomicLong registrationOrder;
    private final AtomicLong mutationVersion;
    private volatile EventErrorHandler errorHandler;

    public EventManager() {
        this(DEFAULT_ERROR_HANDLER);
    }

    public EventManager(EventErrorHandler errorHandler) {
        this.eventHandlers = new ConcurrentHashMap<Class<? extends Event>, CopyOnWriteArrayList<Handler>>();
        this.dispatchCache = new ConcurrentHashMap<Class<?>, CachedDispatch>();
        this.invokerFactories = new ConcurrentHashMap<Method, InvokerFactory>();
        this.listenerPlans = new ConcurrentHashMap<Class<?>, ListenerPlan>();
        this.registrationOrder = new AtomicLong();
        this.mutationVersion = new AtomicLong();
        this.errorHandler = errorHandler != null ? errorHandler : DEFAULT_ERROR_HANDLER;
    }

    /**
     * Replaces the handler invoked when a listener throws. Passing {@code null} restores the default
     * (logging) behaviour.
     *
     * @param errorHandler replacement handler, or {@code null} for the default
     */
    public void setErrorHandler(EventErrorHandler errorHandler) {
        this.errorHandler = errorHandler != null ? errorHandler : DEFAULT_ERROR_HANDLER;
    }

    public void register(Object... listeners) {
        if (listeners == null) {
            return;
        }
        for (Object listener : listeners) {
            register(listener);
        }
    }

    public void register(Object listener) {
        register(listener, null);
    }

    /**
     * Registers only handlers of {@code listener} whose event type is exactly {@code eventClass}.
     * A {@code null} eventClass registers every handler.
     *
     * @param listener listener object to scan
     * @param eventClass exact handler event type, or {@code null} for all types
     */
    public void register(Object listener, Class<? extends Event> eventClass) {
        if (listener == null) {
            return;
        }
        bindListenerPlan(listener, listenerPlanFor(listener.getClass()), eventClass, false);
    }

    /**
     * Registers the <em>static</em> {@link EventTarget} methods and fields of {@code listenerClass} without needing
     * an instance. Undo with {@link #unregister(Class)}.
     *
     * @param listenerClass class containing static handlers
     */
    public void register(Class<?> listenerClass) {
        if (listenerClass == null) {
            return;
        }
        bindListenerPlan(listenerClass, listenerPlanFor(listenerClass), null, true);
    }

    /**
     * Registers a functional listener for events of exactly {@code eventType} (and its subtypes,
     * through normal hierarchy dispatch) at {@link Priority#NORMAL}.
     *
     * @param eventType event type to register
     * @param action listener action
     * @param <T> event type
     * @return a subscription used to remove this listener again
     */
    public <T extends Event> Subscription register(Class<T> eventType, Consumer<? super T> action) {
        return register(eventType, DEFAULT_PRIORITY, false, action);
    }

    /**
     * Registers a functional listener with an explicit priority (lower runs first).
     *
     * @param eventType event type to register
     * @param priority listener priority
     * @param action listener action
     * @param <T> event type
     * @return a subscription used to remove this listener again
     */
    public <T extends Event> Subscription register(Class<T> eventType, int priority, Consumer<? super T> action) {
        return register(eventType, priority, false, action);
    }

    /**
     * Registers a functional listener with an explicit priority and cancellation behaviour.
     *
     * @param eventType event type to register
     * @param priority listener priority
     * @param ignoreCancelled if true, the listener is skipped once a cancellable event has been cancelled
     * @param action listener action
     * @param <T> event type
     * @return a subscription used to remove this listener again
     */
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
                priority,
                ignoreCancelled,
                registrationOrder.getAndIncrement(),
                new Invoker() {
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
     * Registers a typed listener directly, using {@link EventListener#getPriority()}.
     *
     * @param eventType event type to register
     * @param listener typed listener
     * @param <T> event type
     * @return a subscription used to remove this listener again
     */
    public <T extends Event> Subscription registerListener(Class<T> eventType,
                                                           EventListener<? super T> listener) {
        if (eventType == null || listener == null) {
            return Subscription.NOOP;
        }
        return registerListener(eventType, listener.getPriority(), false, listener);
    }

    /**
     * Registers a typed listener directly with an explicit priority.
     *
     * @param eventType event type to register
     * @param priority listener priority
     * @param listener typed listener
     * @param <T> event type
     * @return a subscription used to remove this listener again
     */
    public <T extends Event> Subscription registerListener(Class<T> eventType, int priority,
                                                           EventListener<? super T> listener) {
        return registerListener(eventType, priority, false, listener);
    }

    /**
     * Registers a typed listener directly with explicit priority and cancellation behaviour.
     *
     * @param eventType event type to register
     * @param priority listener priority
     * @param ignoreCancelled whether cancelled events should be ignored
     * @param listener typed listener
     * @param <T> event type
     * @return a subscription used to remove this listener again
     */
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
                priority,
                ignoreCancelled,
                registrationOrder.getAndIncrement(),
                new Invoker() {
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

    /**
     * Registers one typed listener for multiple event types, using
     * {@link EventListener#getPriority()}.
     *
     * <p>The listener must be able to accept the common type {@code T} of every supplied event
     * class. Duplicate and {@code null} event classes are ignored.
     *
     * @param listener typed listener
     * @param eventTypes event types to register
     * @param <T> common event type accepted by the listener
     * @return grouped subscription for every unique event type
     */
    @SafeVarargs
    public final <T extends Event> Subscription registerListener(
            EventListener<? super T> listener, Class<? extends T>... eventTypes) {
        if (listener == null) {
            return Subscription.NOOP;
        }
        return registerListener(listener.getPriority(), false, listener, eventTypes);
    }

    /**
     * Registers one typed listener for multiple event types with an explicit priority.
     *
     * @param priority listener priority
     * @param listener typed listener
     * @param eventTypes event types to register
     * @param <T> common event type accepted by the listener
     * @return grouped subscription for every unique event type
     */
    @SafeVarargs
    public final <T extends Event> Subscription registerListener(
            int priority, EventListener<? super T> listener, Class<? extends T>... eventTypes) {
        return registerListener(priority, false, listener, eventTypes);
    }

    /**
     * Registers one typed listener for multiple event types with explicit priority and
     * cancellation behaviour.
     *
     * @param priority listener priority
     * @param ignoreCancelled whether cancelled events should be ignored
     * @param listener typed listener
     * @param eventTypes event types to register
     * @param <T> common event type accepted by the listener
     * @return grouped subscription for every unique event type
     */
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

    public void unregister(final Object listener) {
        if (listener == null) {
            return;
        }
        removeHandlers(new Predicate<Handler>() {
            @Override
            public boolean test(Handler handler) {
                return handler.listener == listener;
            }
        });
    }

    /**
     * Unregisters only the handlers of {@code listener} that listen for exactly {@code eventClass}.
     *
     * @param listener listener object to remove
     * @param eventClass exact event type to remove
     */
    public void unregister(final Object listener, final Class<? extends Event> eventClass) {
        if (listener == null || eventClass == null) {
            return;
        }
        removeHandlers(new Predicate<Handler>() {
            @Override
            public boolean test(Handler handler) {
                return handler.listener == listener && handler.eventType == eventClass;
            }
        });
    }

    /**
     * Unregisters every listener that is an instance of {@code listenerClass}, as well as static
     * handlers registered through {@link #register(Class)} for that class.
     *
     * @param listenerClass listener base class or static handler class
     */
    public void unregister(final Class<?> listenerClass) {
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

    public void clear() {
        if (!eventHandlers.isEmpty() || !dispatchCache.isEmpty()) {
            eventHandlers.clear();
            dispatchCache.clear();
            mutationVersion.incrementAndGet();
        }
    }

    /**
     * Removes every registered listener. Alias for {@link #clear()}.
     */
    public void unregisterAll() {
        clear();
    }

    /**
     * Removes every registered listener. Compatibility alias used by several event-bus APIs.
     */
    public void unregisterAllListeners() {
        clear();
    }

    /**
     * Removes all handlers registered exactly for {@code eventType}. Handlers registered for a
     * supertype are left intact and will still receive subtype events.
     *
     * @param eventType exact event bucket to remove
     */
    public void removeEntry(Class<? extends Event> eventType) {
        if (eventType == null) {
            return;
        }
        if (eventHandlers.remove(eventType) != null) {
            dispatchCache.clear();
            mutationVersion.incrementAndGet();
        }
    }

    /**
     * Cleans empty event buckets. Passing {@code false} clears the whole bus for compatibility with
     * older event-bus APIs that exposed this method.
     *
     * @param onlyEmptyEntries whether to retain non-empty event buckets
     */
    public void cleanMap(boolean onlyEmptyEntries) {
        if (!onlyEmptyEntries) {
            clear();
            return;
        }

        boolean removedAny = false;
        for (Class<? extends Event> eventType : eventHandlers.keySet()) {
            CopyOnWriteArrayList<Handler> removed = eventHandlers.computeIfPresent(eventType, (key, handlers) ->
                    handlers.isEmpty() ? null : handlers);
            if (removed == null) {
                removedAny = true;
            }
        }
        if (removedAny) {
            dispatchCache.clear();
            mutationVersion.incrementAndGet();
        }
    }

    /**
     * @param listener exact listener object to query
     * @return true if this exact listener object has at least one registered handler
     */
    public boolean isRegistered(Object listener) {
        if (listener == null) {
            return false;
        }
        if (listener instanceof Class<?>) {
            return isRegistered((Class<?>) listener);
        }
        for (CopyOnWriteArrayList<Handler> handlers : eventHandlers.values()) {
            for (Handler handler : handlers) {
                if (handler.listener == listener) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * @param listenerClass listener base class or static handler class to query
     * @return true if any registered listener is an instance of {@code listenerClass}, or if static
     * handlers were registered for that class
     */
    public boolean isRegistered(Class<?> listenerClass) {
        if (listenerClass == null) {
            return false;
        }
        for (CopyOnWriteArrayList<Handler> handlers : eventHandlers.values()) {
            for (Handler handler : handlers) {
                if (handler.listener == listenerClass
                        || listenerClass.isAssignableFrom(listenerClassOf(handler.listener))) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * @return number of distinct listener objects/classes currently registered
     */
    public int listenerCount() {
        Set<Object> listeners = Collections.newSetFromMap(new IdentityHashMap<Object, Boolean>());
        for (CopyOnWriteArrayList<Handler> handlers : eventHandlers.values()) {
            for (Handler handler : handlers) {
                listeners.add(handler.listener);
            }
        }
        return listeners.size();
    }

    /**
     * @return total number of registered handlers across all event types
     */
    public int handlerCount() {
        int count = 0;
        for (CopyOnWriteArrayList<Handler> handlers : eventHandlers.values()) {
            count += handlers.size();
        }
        return count;
    }

    /**
     * @param eventType runtime event type to query
     * @return number of handlers that would receive an event of {@code eventType}
     */
    public int handlerCount(Class<? extends Event> eventType) {
        return eventType == null ? 0 : handlersFor(eventType).length;
    }

    /**
     * @param eventType exact registered event type to query
     * @return number of handlers registered exactly for {@code eventType}, excluding supertypes
     */
    public int exactHandlerCount(Class<? extends Event> eventType) {
        return eventType == null ? 0 : exactHandlersFor(eventType).length;
    }

    /**
     * @param eventType runtime event type to query
     * @return true if at least one handler would receive an event of the given type
     */
    public boolean hasListeners(Class<? extends Event> eventType) {
        return eventType != null && handlersFor(eventType).length != 0;
    }

    /**
     * @param eventType exact registered event type to query
     * @return true if at least one handler is registered exactly for the given type
     */
    public boolean hasExactListeners(Class<? extends Event> eventType) {
        return eventType != null && exactHandlersFor(eventType).length != 0;
    }

    public <T extends Event> T call(T event) {
        if (event == null) {
            return null;
        }

        Handler[] handlers = handlersFor(event.getClass());
        if (handlers.length == 0) {
            return event;
        }

        dispatch(event, handlers);
        return event;
    }

    /**
     * Dispatches an event and invokes {@code afterDispatch} once after all eligible handlers finish.
     * The callback also runs when no handlers are registered.
     *
     * @param event event to dispatch
     * @param afterDispatch callback invoked after dispatch, or {@code null}
     * @param <T> event type
     * @return the supplied event, or {@code null} when the event is {@code null}
     */
    public <T extends Event> T call(T event, Runnable afterDispatch) {
        if (event == null) {
            return null;
        }
        try {
            return call(event);
        } finally {
            if (afterDispatch != null) {
                afterDispatch.run();
            }
        }
    }

    /**
     * Dispatches only to handlers registered exactly for the event's runtime class.
     *
     * @param event event to dispatch
     * @param <T> event type
     * @return the supplied event, or {@code null} when the event is {@code null}
     */
    public <T extends Event> T callExact(T event) {
        if (event == null) {
            return null;
        }

        Handler[] handlers = exactHandlersFor(event.getClass());
        if (handlers.length != 0) {
            dispatch(event, handlers);
        }
        return event;
    }

    /**
     * Performs exact-type dispatch and invokes {@code afterDispatch} once afterwards.
     *
     * @param event event to dispatch
     * @param afterDispatch callback invoked after dispatch, or {@code null}
     * @param <T> event type
     * @return the supplied event, or {@code null} when the event is {@code null}
     */
    public <T extends Event> T callExact(T event, Runnable afterDispatch) {
        if (event == null) {
            return null;
        }
        try {
            return callExact(event);
        } finally {
            if (afterDispatch != null) {
                afterDispatch.run();
            }
        }
    }

    /**
     * Dispatches an event. Alias for {@link #call(Event)}.
     *
     * @param event event to dispatch
     * @param <T> event type
     * @return the supplied event
     */
    public <T extends Event> T post(T event) {
        return call(event);
    }

    /**
     * Dispatches an event and invokes a completion callback. Alias for {@link #call(Event, Runnable)}.
     *
     * @param event event to dispatch
     * @param afterDispatch callback invoked after dispatch, or {@code null}
     * @param <T> event type
     * @return the supplied event
     */
    public <T extends Event> T post(T event, Runnable afterDispatch) {
        return call(event, afterDispatch);
    }

    /**
     * Dispatches an event. Alias for {@link #call(Event)}.
     *
     * @param event event to dispatch
     * @param <T> event type
     * @return the supplied event
     */
    public <T extends Event> T fire(T event) {
        return call(event);
    }

    /**
     * Dispatches an event and invokes a completion callback. Alias for {@link #call(Event, Runnable)}.
     *
     * @param event event to dispatch
     * @param afterDispatch callback invoked after dispatch, or {@code null}
     * @param <T> event type
     * @return the supplied event
     */
    public <T extends Event> T fire(T event, Runnable afterDispatch) {
        return call(event, afterDispatch);
    }

    /**
     * Lazily constructs and dispatches an event: {@code supplier} is only invoked when at least one
     * handler listens for {@code eventType}.
     *
     * @param eventType event type to query before construction
     * @param supplier lazy event factory
     * @param <T> event type
     * @return the dispatched event, or {@code null} if nothing listens and no event was created
     */
    public <T extends Event> T call(Class<T> eventType, Supplier<T> supplier) {
        if (eventType == null || supplier == null || !hasListeners(eventType)) {
            return null;
        }
        return call(supplier.get());
    }

    /**
     * Lazily constructs and exactly dispatches an event.
     *
     * @param eventType exact event type to query before construction
     * @param supplier lazy event factory
     * @param <T> event type
     * @return the dispatched event, or {@code null} if no exact listener exists
     */
    public <T extends Event> T callExact(Class<T> eventType, Supplier<T> supplier) {
        if (eventType == null || supplier == null || !hasExactListeners(eventType)) {
            return null;
        }
        return callExact(supplier.get());
    }

    /**
     * Lazily constructs and dispatches an event. Alias for {@link #call(Class, Supplier)}.
     *
     * @param eventType event type to query before construction
     * @param supplier lazy event factory
     * @param <T> event type
     * @return the dispatched event, or {@code null} if nothing listens
     */
    public <T extends Event> T post(Class<T> eventType, Supplier<T> supplier) {
        return call(eventType, supplier);
    }

    /**
     * Lazily constructs and dispatches an event. Alias for {@link #call(Class, Supplier)}.
     *
     * @param eventType event type to query before construction
     * @param supplier lazy event factory
     * @param <T> event type
     * @return the dispatched event, or {@code null} if nothing listens
     */
    public <T extends Event> T fire(Class<T> eventType, Supplier<T> supplier) {
        return call(eventType, supplier);
    }

    private void dispatch(Event event, Handler[] handlers) {
        Cancellable cancellable = event instanceof Cancellable ? (Cancellable) event : null;
        Stoppable stoppable = event instanceof Stoppable ? (Stoppable) event : null;

        for (int i = 0; i < handlers.length; i++) {
            Handler handler = handlers[i];

            if (stoppable != null && stoppable.isStopped()) {
                break;
            }

            if (!handler.isHandlingEvents()) {
                continue;
            }

            if (cancellable != null && cancellable.isCancelled() && handler.ignoreCancelled) {
                continue;
            }

            try {
                handler.invoke(event);
            } catch (Throwable t) {
                try {
                    errorHandler.handle(event, handler.listener, t);
                } catch (Throwable errorHandlerFailure) {
                    LOGGER.log(Level.SEVERE, "Event error handler threw while handling a listener failure",
                            errorHandlerFailure);
                }
            }
        }
    }

    private Handler[] handlersFor(Class<?> eventClass) {
        long version = mutationVersion.get();
        CachedDispatch cached = dispatchCache.get(eventClass);
        if (cached != null && cached.version == version) {
            return cached.handlers;
        }

        Handler[] built = buildDispatchList(eventClass);
        dispatchCache.put(eventClass, new CachedDispatch(version, built));
        return built;
    }

    private Handler[] exactHandlersFor(Class<?> eventClass) {
        List<Handler> handlers = eventHandlers.get(eventClass);
        return handlers == null || handlers.isEmpty() ? NO_HANDLERS : handlers.toArray(NO_HANDLERS);
    }

    private Handler[] buildDispatchList(Class<?> eventClass) {
        LinkedHashSet<Handler> collected = new LinkedHashSet<Handler>();
        collectHandlers(eventClass, collected, new HashSet<Class<?>>());

        if (collected.isEmpty()) {
            return NO_HANDLERS;
        }

        List<Handler> handlers = new ArrayList<Handler>(collected);
        handlers.sort(HANDLER_ORDER);
        return handlers.toArray(NO_HANDLERS);
    }

    private ListenerPlan listenerPlanFor(final Class<?> listenerClass) {
        return listenerPlans.computeIfAbsent(listenerClass,
                new java.util.function.Function<Class<?>, ListenerPlan>() {
                    @Override
                    public ListenerPlan apply(Class<?> type) {
                        return buildListenerPlan(type);
                    }
                });
    }

    private ListenerPlan buildListenerPlan(Class<?> listenerClass) {
        List<HandlerDefinition> definitions = new ArrayList<HandlerDefinition>();
        scanListenerType(listenerClass, definitions, new HashSet<Class<?>>(),
                new java.util.HashMap<MethodSignature, List<Method>>());
        return new ListenerPlan(definitions.toArray(new HandlerDefinition[definitions.size()]));
    }

    private void scanListenerType(Class<?> type, List<HandlerDefinition> definitions,
                                  Set<Class<?>> visitedTypes,
                                  Map<MethodSignature, List<Method>> seenMethods) {
        if (type == null || type == Object.class || !visitedTypes.add(type)) {
            return;
        }

        for (Method method : type.getDeclaredMethods()) {
            if (method.isSynthetic() || method.isBridge()) {
                continue;
            }

            MethodSignature signature = new MethodSignature(method);
            List<Method> descendants = seenMethods.get(signature);
            boolean shadowed = isShadowedBy(method, descendants);
            if (descendants == null) {
                descendants = new ArrayList<Method>();
                seenMethods.put(signature, descendants);
            }
            descendants.add(method);

            // An overriding declaration on a subtype shadows the inherited handler even when the
            // override deliberately omits @EventTarget.
            if (shadowed || !method.isAnnotationPresent(EventTarget.class)) {
                continue;
            }
            if (method.getParameterTypes().length != 1) {
                LOGGER.warning("Skipping handler " + method + " because it must have exactly one parameter");
                continue;
            }
            if (method.getReturnType() != Void.TYPE) {
                LOGGER.warning("Skipping handler " + method + " because handler methods must return void");
                continue;
            }

            Class<?> parameterType = method.getParameterTypes()[0];
            if (!Event.class.isAssignableFrom(parameterType)) {
                LOGGER.warning("Skipping handler " + method + " because parameter type is not an Event");
                continue;
            }

            EventTarget eventTarget = method.getAnnotation(EventTarget.class);
            EventPriority priorityAnnotation = method.getAnnotation(EventPriority.class);
            int priority = priorityAnnotation != null
                    ? priorityAnnotation.value()
                    : annotationPriority(eventTarget, DEFAULT_PRIORITY);
            definitions.add(HandlerDefinition.forMethod(
                    method,
                    parameterType.asSubclass(Event.class),
                    priority,
                    eventTarget.ignoreCancelled()
            ));
        }

        for (Field field : type.getDeclaredFields()) {
            if (field.isSynthetic() || !field.isAnnotationPresent(EventTarget.class)) {
                continue;
            }
            if (!EventListener.class.isAssignableFrom(field.getType())) {
                LOGGER.warning("Skipping listener field " + field + " because it is not an EventListener");
                continue;
            }

            Class<? extends Event> fieldEventType = eventTypeFromField(field);
            if (fieldEventType == null) {
                LOGGER.warning("Skipping listener field " + field
                        + " because its EventListener event type could not be inferred");
                continue;
            }

            EventTarget eventTarget = field.getAnnotation(EventTarget.class);
            EventPriority priorityAnnotation = field.getAnnotation(EventPriority.class);
            boolean listenerPriority = priorityAnnotation == null
                    && eventTarget.value() == Priority.UNSPECIFIED;
            int priority = priorityAnnotation != null
                    ? priorityAnnotation.value()
                    : annotationPriority(eventTarget, DEFAULT_PRIORITY);
            definitions.add(HandlerDefinition.forField(
                    field,
                    fieldEventType,
                    priority,
                    listenerPriority,
                    eventTarget.ignoreCancelled()
            ));
        }

        for (Class<?> iface : type.getInterfaces()) {
            scanListenerType(iface, definitions, visitedTypes, seenMethods);
        }
        scanListenerType(type.getSuperclass(), definitions, visitedTypes, seenMethods);
    }

    private static int annotationPriority(EventTarget eventTarget, int fallback) {
        return eventTarget.value() != Priority.UNSPECIFIED ? eventTarget.value() : fallback;
    }

    private static boolean isShadowedBy(Method inheritedMethod, List<Method> descendants) {
        if (descendants == null) {
            return false;
        }
        for (Method descendant : descendants) {
            if (shadows(descendant, inheritedMethod)) {
                return true;
            }
        }
        return false;
    }

    private static boolean shadows(Method descendant, Method inheritedMethod) {
        Class<?> descendantClass = descendant.getDeclaringClass();
        Class<?> inheritedClass = inheritedMethod.getDeclaringClass();
        if (descendantClass == inheritedClass) {
            return true;
        }

        if (!inheritedClass.isAssignableFrom(descendantClass)) {
            // Two unrelated interfaces with the same signature still describe one effective
            // listener method on an implementing class.
            return inheritedClass.isInterface() && descendantClass.isInterface();
        }

        int inheritedModifiers = inheritedMethod.getModifiers();
        if (Modifier.isPrivate(inheritedModifiers)) {
            return false;
        }

        int descendantModifiers = descendant.getModifiers();
        boolean inheritedStatic = Modifier.isStatic(inheritedModifiers);
        boolean descendantStatic = Modifier.isStatic(descendantModifiers);
        if (inheritedStatic || descendantStatic) {
            return inheritedStatic && descendantStatic;
        }

        if (isPackagePrivate(inheritedModifiers)
                && !packageName(inheritedClass).equals(packageName(descendantClass))) {
            return false;
        }
        return true;
    }

    private static boolean isPackagePrivate(int modifiers) {
        return !Modifier.isPublic(modifiers)
                && !Modifier.isProtected(modifiers)
                && !Modifier.isPrivate(modifiers);
    }

    private static String packageName(Class<?> type) {
        Package typePackage = type.getPackage();
        return typePackage == null ? "" : typePackage.getName();
    }

    private void bindListenerPlan(Object listener, ListenerPlan plan, Class<? extends Event> eventClass,
                                  boolean staticOnly) {
        for (HandlerDefinition definition : plan.definitions) {
            if (staticOnly && !definition.staticMember) {
                continue;
            }
            if (eventClass != null && definition.eventType != eventClass) {
                continue;
            }

            if (definition.method != null) {
                Method method = definition.method;
                Handler handler = new Handler(
                        listener,
                        method,
                        null,
                        definition.staticMember ? method.getDeclaringClass() : listener,
                        definition.eventType,
                        definition.priority,
                        definition.ignoreCancelled,
                        registrationOrder.getAndIncrement(),
                        invokerFor(method, listener)
                );
                addHandler(handler);
                continue;
            }

            Field field = definition.field;
            EventListener<?> fieldListener = listenerFromField(listener, field);
            if (fieldListener == null) {
                continue;
            }

            final Class<? extends Event> dispatchType = definition.eventType;
            final EventListener<?> dispatchListener = fieldListener;
            int priority = definition.listenerPriority
                    ? fieldListener.getPriority()
                    : definition.priority;

            Handler handler = new Handler(
                    listener,
                    null,
                    field,
                    definition.staticMember ? field.getDeclaringClass() : listener,
                    dispatchType,
                    priority,
                    definition.ignoreCancelled,
                    registrationOrder.getAndIncrement(),
                    new Invoker() {
                        @SuppressWarnings({"unchecked", "rawtypes"})
                        @Override
                        public void invoke(Event event) {
                            ((EventListener) dispatchListener).onEvent(dispatchType.cast(event));
                        }
                    }
            );
            addHandler(handler);
        }
    }

    private void addHandler(final Handler handler) {
        final boolean[] added = new boolean[1];
        eventHandlers.compute(handler.eventType, (key, existing) -> {
            CopyOnWriteArrayList<Handler> handlers = existing != null
                    ? existing
                    : new CopyOnWriteArrayList<Handler>();
            if (!handlers.contains(handler)) {
                int index = 0;
                while (index < handlers.size() && HANDLER_ORDER.compare(handlers.get(index), handler) <= 0) {
                    index++;
                }
                handlers.add(index, handler);
                added[0] = true;
            }
            return handlers;
        });
        if (added[0]) {
            mutationVersion.incrementAndGet();
        }
    }

    private void removeHandlers(final Predicate<Handler> predicate) {
        final boolean[] removed = new boolean[1];
        for (Class<? extends Event> eventType : eventHandlers.keySet()) {
            eventHandlers.computeIfPresent(eventType, (key, handlers) -> {
                if (handlers.removeIf(predicate)) {
                    removed[0] = true;
                }
                return handlers.isEmpty() ? null : handlers;
            });
        }
        if (removed[0]) {
            dispatchCache.clear();
            mutationVersion.incrementAndGet();
        }
    }

    private void collectHandlers(Class<?> type, Set<Handler> collected, Set<Class<?>> visitedTypes) {
        if (type == null || type == Object.class || !Event.class.isAssignableFrom(type) || !visitedTypes.add(type)) {
            return;
        }

        @SuppressWarnings("unchecked")
        Class<? extends Event> eventType = (Class<? extends Event>) type;
        List<Handler> handlers = eventHandlers.get(eventType);
        if (handlers != null) {
            collected.addAll(handlers);
        }

        for (Class<?> iface : type.getInterfaces()) {
            collectHandlers(iface, collected, visitedTypes);
        }
        collectHandlers(type.getSuperclass(), collected, visitedTypes);
    }

    private EventListener<?> listenerFromField(Object listener, Field field) {
        boolean isStatic = Modifier.isStatic(field.getModifiers());
        try {
            field.setAccessible(true);
        } catch (SecurityException ignored) {
            // Fall through: public fields can still be read.
        }

        try {
            Object value = field.get(isStatic ? null : listener);
            if (value == null) {
                LOGGER.warning("Skipping listener field " + field + " because its value is null");
                return null;
            }
            if (!(value instanceof EventListener<?>)) {
                LOGGER.warning("Skipping listener field " + field + " because its value is not an EventListener");
                return null;
            }
            return (EventListener<?>) value;
        } catch (IllegalAccessException e) {
            LOGGER.log(Level.WARNING, "Skipping listener field " + field + " because it is not accessible", e);
            return null;
        }
    }

    private static Class<? extends Event> eventTypeFromField(Field field) {
        Type genericType = field.getGenericType();
        if (!(genericType instanceof ParameterizedType)) {
            return null;
        }

        ParameterizedType parameterizedType = (ParameterizedType) genericType;
        Type rawType = parameterizedType.getRawType();
        if (!(rawType instanceof Class<?>) || !EventListener.class.isAssignableFrom((Class<?>) rawType)) {
            return null;
        }

        Type eventType = parameterizedType.getActualTypeArguments()[0];
        Class<?> eventClass = classFromType(eventType);
        if (eventClass == null || !Event.class.isAssignableFrom(eventClass)) {
            return null;
        }
        return eventClass.asSubclass(Event.class);
    }

    private static Class<?> classFromType(Type type) {
        if (type instanceof Class<?>) {
            return (Class<?>) type;
        }
        if (type instanceof ParameterizedType) {
            Type rawType = ((ParameterizedType) type).getRawType();
            return rawType instanceof Class<?> ? (Class<?>) rawType : null;
        }
        if (type instanceof WildcardType) {
            WildcardType wildcardType = (WildcardType) type;
            Type[] lowerBounds = wildcardType.getLowerBounds();
            if (lowerBounds.length > 0) {
                return classFromType(lowerBounds[0]);
            }
            Type[] upperBounds = wildcardType.getUpperBounds();
            if (upperBounds.length > 0) {
                return classFromType(upperBounds[0]);
            }
        }
        return null;
    }

    private static Class<?> listenerClassOf(Object listener) {
        return listener instanceof Class<?> ? (Class<?>) listener : listener.getClass();
    }

    private Invoker invokerFor(Method method, Object listener) {
        InvokerFactory factory = invokerFactories.computeIfAbsent(method, new java.util.function.Function<Method, InvokerFactory>() {
            @Override
            public InvokerFactory apply(Method key) {
                return buildInvokerFactory(key);
            }
        });
        return factory.create(listener);
    }

    /**
     * Builds a per-method factory of invokers so the expensive lookup / lambda generation runs once
     * per method, no matter how many instances register or how often they toggle.
     */
    private static InvokerFactory buildInvokerFactory(final Method method) {
        final boolean isStatic = Modifier.isStatic(method.getModifiers());

        try {
            method.setAccessible(true);
        } catch (SecurityException ignored) {
            // Fall through: accessible handlers can still work without this.
        }

        MethodHandles.Lookup lookup = lookupFor(method.getDeclaringClass());

        // Fastest path: compile the handler into a Consumer via LambdaMetafactory. With a private
        // lookup (Java 9+) this covers private handlers too; otherwise fall back below.
        try {
            MethodHandle target = lookup.unreflect(method);
            MethodType invokedType = isStatic
                    ? MethodType.methodType(Consumer.class)
                    : MethodType.methodType(Consumer.class, method.getDeclaringClass());
            final MethodHandle factory = LambdaMetafactory.metafactory(
                    lookup,
                    "accept",
                    invokedType,
                    MethodType.methodType(void.class, Object.class),
                    target,
                    MethodType.methodType(void.class, method.getParameterTypes()[0])
            ).getTarget();

            if (isStatic) {
                @SuppressWarnings("unchecked")
                final Consumer<Event> shared = (Consumer<Event>) factory.invoke();
                final Invoker invoker = new Invoker() {
                    @Override
                    public void invoke(Event event) {
                        shared.accept(event);
                    }
                };
                return new InvokerFactory() {
                    @Override
                    public Invoker create(Object listener) {
                        return invoker;
                    }
                };
            }

            return new InvokerFactory() {
                @Override
                public Invoker create(Object listener) {
                    try {
                        @SuppressWarnings("unchecked")
                        final Consumer<Event> consumer = (Consumer<Event>) factory.invoke(listener);
                        return new Invoker() {
                            @Override
                            public void invoke(Event event) {
                                consumer.accept(event);
                            }
                        };
                    } catch (Throwable t) {
                        return reflectiveInvoker(method, listener, false);
                    }
                }
            };
        } catch (Throwable ignored) {
            // Fall through to MethodHandle.
        }

        try {
            final MethodHandle handle = lookup.unreflect(method);
            if (isStatic) {
                final Invoker invoker = new Invoker() {
                    @Override
                    public void invoke(Event event) throws Throwable {
                        handle.invoke(event);
                    }
                };
                return new InvokerFactory() {
                    @Override
                    public Invoker create(Object listener) {
                        return invoker;
                    }
                };
            }
            return new InvokerFactory() {
                @Override
                public Invoker create(Object listener) {
                    final MethodHandle bound = handle.bindTo(listener);
                    return new Invoker() {
                        @Override
                        public void invoke(Event event) throws Throwable {
                            bound.invoke(event);
                        }
                    };
                }
            };
        } catch (IllegalAccessException ignored) {
            // Fall through to reflection.
        }

        return new InvokerFactory() {
            @Override
            public Invoker create(Object listener) {
                return reflectiveInvoker(method, listener, isStatic);
            }
        };
    }

    private static Invoker reflectiveInvoker(final Method method, final Object listener, final boolean isStatic) {
        return new Invoker() {
            @Override
            public void invoke(Event event) throws Throwable {
                try {
                    method.invoke(isStatic ? null : listener, event);
                } catch (InvocationTargetException invocationFailure) {
                    Throwable cause = invocationFailure.getCause();
                    throw cause != null ? cause : invocationFailure;
                }
            }
        };
    }

    private static Method resolvePrivateLookupIn() {
        try {
            return MethodHandles.class.getMethod("privateLookupIn", Class.class, MethodHandles.Lookup.class);
        } catch (NoSuchMethodException javaEight) {
            return null;
        }
    }

    private static MethodHandles.Lookup lookupFor(Class<?> declaringClass) {
        if (PRIVATE_LOOKUP_IN != null) {
            try {
                return (MethodHandles.Lookup) PRIVATE_LOOKUP_IN.invoke(null, declaringClass, LOOKUP);
            } catch (Throwable ignored) {
                // Inaccessible module etc.; the plain lookup still handles public handlers.
            }
        }
        return LOOKUP;
    }

    private interface Invoker {
        void invoke(Event event) throws Throwable;
    }

    private interface InvokerFactory {
        Invoker create(Object listener);
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
            removeHandlers(new Predicate<Handler>() {
                @Override
                public boolean test(Handler candidate) {
                    return candidate == handler;
                }
            });
        }

        @Override
        public boolean isSubscribed() {
            if (!subscribed.get()) {
                return false;
            }
            List<Handler> handlers = eventHandlers.get(handler.eventType);
            if (handlers == null) {
                return false;
            }
            for (Handler candidate : handlers) {
                if (candidate == handler) {
                    return true;
                }
            }
            return false;
        }
    }

    private static final class ListenerPlan {
        private final HandlerDefinition[] definitions;

        private ListenerPlan(HandlerDefinition[] definitions) {
            this.definitions = definitions;
        }
    }

    private static final class HandlerDefinition {
        private final Method method;
        private final Field field;
        private final Class<? extends Event> eventType;
        private final int priority;
        private final boolean listenerPriority;
        private final boolean ignoreCancelled;
        private final boolean staticMember;

        private HandlerDefinition(Method method, Field field, Class<? extends Event> eventType,
                                  int priority, boolean listenerPriority, boolean ignoreCancelled,
                                  boolean staticMember) {
            this.method = method;
            this.field = field;
            this.eventType = eventType;
            this.priority = priority;
            this.listenerPriority = listenerPriority;
            this.ignoreCancelled = ignoreCancelled;
            this.staticMember = staticMember;
        }

        private static HandlerDefinition forMethod(Method method, Class<? extends Event> eventType,
                                                   int priority, boolean ignoreCancelled) {
            return new HandlerDefinition(
                    method,
                    null,
                    eventType,
                    priority,
                    false,
                    ignoreCancelled,
                    Modifier.isStatic(method.getModifiers())
            );
        }

        private static HandlerDefinition forField(Field field, Class<? extends Event> eventType,
                                                  int priority, boolean listenerPriority,
                                                  boolean ignoreCancelled) {
            return new HandlerDefinition(
                    null,
                    field,
                    eventType,
                    priority,
                    listenerPriority,
                    ignoreCancelled,
                    Modifier.isStatic(field.getModifiers())
            );
        }
    }

    private static final class CachedDispatch {
        private final long version;
        private final Handler[] handlers;

        private CachedDispatch(long version, Handler[] handlers) {
            this.version = version;
            this.handlers = handlers;
        }
    }

    private static final class Handler {
        private final Object listener;
        private final Method method;
        private final Field field;
        private final Object dedupeOwner;
        private final Class<? extends Event> eventType;
        private final int priority;
        private final boolean ignoreCancelled;
        private final long order;
        private final Invoker invoker;

        private Handler(Object listener, Method method, Field field, Object dedupeOwner,
                        Class<? extends Event> eventType, int priority, boolean ignoreCancelled,
                        long order, Invoker invoker) {
            this.listener = listener;
            this.method = method;
            this.field = field;
            this.dedupeOwner = dedupeOwner;
            this.eventType = eventType;
            this.priority = priority;
            this.ignoreCancelled = ignoreCancelled;
            this.order = order;
            this.invoker = invoker;
        }

        private void invoke(Event event) throws Throwable {
            invoker.invoke(event);
        }

        private boolean isHandlingEvents() {
            return !(listener instanceof EventSubscriber) || ((EventSubscriber) listener).isHandlingEvents();
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
            // Direct functional registrations have no reflective member and may be registered
            // multiple times deliberately.
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

    private static final class MethodSignature {
        private final String name;
        private final Class<?>[] parameterTypes;
        private final int hashCode;

        private MethodSignature(Method method) {
            this.name = method.getName();
            this.parameterTypes = method.getParameterTypes();
            this.hashCode = 31 * name.hashCode() + java.util.Arrays.hashCode(parameterTypes);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof MethodSignature)) {
                return false;
            }
            MethodSignature other = (MethodSignature) obj;
            return name.equals(other.name) && java.util.Arrays.equals(parameterTypes, other.parameterTypes);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }
    }
}
