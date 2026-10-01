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
    static final int DEFAULT_PRIORITY = Priority.NORMAL;
    static final EventErrorHandler DEFAULT_ERROR_HANDLER = new EventErrorHandler() {
        @Override
        public void handle(Event event, Object listener, Throwable throwable) {
            log.log(Level.SEVERE, "Failed to dispatch " + event.getClass().getName()
                    + " to listener " + listener, throwable);
        }
    };

    final UpcasterRegistry upcasterRegistry = new UpcasterRegistry();
    final HandlerRegistry handlerRegistry = new HandlerRegistry(this);
    final DispatchPipeline dispatchPipeline = new DispatchPipeline(this);
    final String name;
    final Executor defaultExecutor;
    final BusMetrics busMetrics = new BusMetrics();
    volatile boolean deadEventsEnabled = true;
    volatile boolean closed = false;
    private volatile Predicate<Thread> threadEnforcer;
    final EventManager parent;
    private final Set<EventManager> children = Collections.newSetFromMap(new ConcurrentHashMap<EventManager, Boolean>());
    final StickyEventStore stickyStore = new StickyEventStore(upcasterRegistry);



    volatile EventErrorHandler errorHandler = DEFAULT_ERROR_HANDLER;
    volatile ErrorPolicy errorPolicy = ErrorPolicy.CONTINUE;
    volatile EventInterceptor interceptor;
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
        child.setMetricsEnabled(this.busMetrics.isEnabled());
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
        return busMetrics.isEnabled();
    }

    public void setMetricsEnabled(boolean metricsEnabled) {
        busMetrics.setEnabled(metricsEnabled);
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

    // ==========================================
    // Dispatch API (delegates to DispatchPipeline)
    // ==========================================

    /**
     * Dispatches an event to all matching handlers and returns it.
     *
     * <p>Post-dispatch precedence: when upcasters are registered for the event's
     * type, the converted events are dispatched afterwards, and the original
     * event is then considered handled — it neither bubbles to the owner.parent bus
     * nor produces a {@link DeadEvent}. Otherwise the event bubbles to the
     * owner.parent bus (unless cancelled or stopped), and only if no owner.parent and no
     * handler exists may a {@link DeadEvent} be emitted.
     *
     * @param event the event to dispatch
     * @param <T>   the event type
     * @return the dispatched event, for chaining
     */
    public <T extends Event> T dispatch(T event) {
        return dispatchPipeline.dispatch(event);
    }

    public <T extends Event> T dispatch(T event, Runnable afterDispatch) {
        return dispatchPipeline.dispatch(event, afterDispatch);
    }

    public <T extends Event> T dispatchExact(T event) {
        return dispatchPipeline.dispatchExact(event);
    }

    public <T extends Event> T dispatchExact(T event, Runnable afterDispatch) {
        return dispatchPipeline.dispatchExact(event, afterDispatch);
    }

    /**
     * Dispatches multiple events in sequential order.
     */
    public void dispatchAll(Event... events) {
        dispatchPipeline.dispatchAll(events);
    }

    /**
     * Dispatches an iterable collection of events in sequential order.
     */
    public void dispatchAll(Iterable<? extends Event> events) {
        dispatchPipeline.dispatchAll(events);
    }

    /**
     * Dispatches an event asynchronously using ForkJoinPool.commonPool().
     */
    public <T extends Event> CompletableFuture<T> dispatchAsync(final T event) {
        return dispatchPipeline.dispatchAsync(event);
    }

    /**
     * Dispatches an event through the supplied executor. The returned future
     * completes with the same event instance after dispatch finishes.
     */
    public <T extends Event> CompletableFuture<T> dispatchAsync(final T event, Executor executor) {
        return dispatchPipeline.dispatchAsync(event, executor);
    }

    /**
     * Dispatches an event to exact-type handlers asynchronously using ForkJoinPool.commonPool().
     */
    public <T extends Event> CompletableFuture<T> dispatchExactAsync(final T event) {
        return dispatchPipeline.dispatchExactAsync(event);
    }

    /**
     * Dispatches an event to exact-type handlers through the supplied executor.
     */
    public <T extends Event> CompletableFuture<T> dispatchExactAsync(final T event, Executor executor) {
        return dispatchPipeline.dispatchExactAsync(event, executor);
    }

    public <T extends Event> T dispatch(Class<T> eventType, Supplier<T> supplier) {
        return dispatchPipeline.dispatch(eventType, supplier);
    }

    public <T extends Event> T dispatchExact(Class<T> eventType, Supplier<T> supplier) {
        return dispatchPipeline.dispatchExact(eventType, supplier);
    }

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
        return dispatchPipeline.dispatch(event, context);
    }

    /**
     * Dispatches an event to exact handlers within the scope of the specified {@link EventContext}.
     */
    public <T extends Event> T dispatchExact(T event, EventContext context) {
        return dispatchPipeline.dispatchExact(event, context);
    }

    /**
     * Dispatches an event and caches it as the latest sticky event for its concrete runtime type.
     * Newly registered listeners marked as sticky will immediately receive this cached event upon registration.
     *
     * @param event the event to dispatch and cache
     * @param <T>   the event type
     * @return the dispatched event
     */
    public <T extends Event> T dispatchSticky(T event) {
        return dispatchPipeline.dispatchSticky(event);
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

    void checkThreadAffinity(Event event) {
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
    void ensureOpen() {
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
        return busMetrics.snapshot();
    }

    /** Returns counters for one runtime event type without retaining its class loader forever. */
    public EventMetrics metrics(Class<? extends Event> eventType) {
        return busMetrics.snapshot(eventType);
    }

    /** Resets dispatch counters without changing registrations. */
    public void resetMetrics() {
        busMetrics.reset();
    }

    // ==========================================
    // Registration API (delegates to HandlerRegistry)
    // ==========================================

    public void register(Object... listeners) {
        handlerRegistry.register(listeners);
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
        return handlerRegistry.register(listener);
    }

    /**
     * Registers a listener, optionally restricted to handlers bound for the
     * exact event class. Consumer listeners require a non-null event class and
     * may be registered multiple times (each call yields an independent
     * subscription); annotated listeners are deduplicated per instance.
     */
    public Subscription register(Object listener, Class<? extends Event> eventClass) {
        return handlerRegistry.register(listener, eventClass);
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
        return handlerRegistry.registerWeak(listener);
    }

    /**
     * Registers an object listener weakly for a specific event class.
     */
    public Subscription registerWeak(Object listener, Class<? extends Event> eventClass) {
        return handlerRegistry.registerWeak(listener, eventClass);
    }

    /**
     * Registers all static handlers of a listener class.
     */
    public Subscription register(Class<?> listenerClass) {
        return handlerRegistry.register(listenerClass);
    }

    /**
     * Registers all static handlers of a listener class, optionally restricted
     * to the exact event class. Registering the same class again is a no-op.
     */
    public Subscription register(Class<?> listenerClass, Class<? extends Event> eventClass) {
        return handlerRegistry.register(listenerClass, eventClass);
    }

    public Subscription register(Object listener, Method method) {
        return handlerRegistry.register(listener, method);
    }

    public Subscription register(Object listener, Field field) {
        return handlerRegistry.register(listener, field);
    }

    public <T extends Event> Subscription register(Class<T> eventType, Consumer<? super T> action) {
        return handlerRegistry.register(eventType, action);
    }

    public <T extends Event> Subscription register(Class<T> eventType, int priority, Consumer<? super T> action) {
        return handlerRegistry.register(eventType, priority, action);
    }

    public <T extends Event> Subscription registerOnce(Class<T> eventType, Consumer<? super T> action) {
        return handlerRegistry.registerOnce(eventType, action);
    }

    public <T extends Event> Subscription registerOnce(Class<T> eventType, int priority,
                                                       Consumer<? super T> action) {
        return handlerRegistry.registerOnce(eventType, priority, action);
    }

    public <T extends Event> Subscription registerOnce(final Class<T> eventType, int priority,
                                                       boolean ignoreCancelled,
                                                       final Consumer<? super T> action) {
        return handlerRegistry.registerOnce(eventType, priority, ignoreCancelled, action);
    }

    public <T extends Event> Subscription registerFiltered(Class<T> eventType,
                                                            Predicate<? super T> filter,
                                                            Consumer<? super T> action) {
        return handlerRegistry.registerFiltered(eventType, filter, action);
    }

    public <T extends Event> Subscription registerFiltered(Class<T> eventType, int priority,
                                                            Predicate<? super T> filter,
                                                            Consumer<? super T> action) {
        return handlerRegistry.registerFiltered(eventType, priority, filter, action);
    }

    public <T extends Event> Subscription registerFiltered(final Class<T> eventType, int priority,
                                                            boolean ignoreCancelled,
                                                            final Predicate<? super T> filter,
                                                            final Consumer<? super T> action) {
        return handlerRegistry.registerFiltered(eventType, priority, ignoreCancelled, filter, action);
    }

    public <T extends Event> Subscription register(final Class<T> eventType, int priority,
                                                   boolean ignoreCancelled, final Consumer<? super T> action) {
        return handlerRegistry.register(eventType, priority, ignoreCancelled, action);
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
        return handlerRegistry.registerWeak(eventType, action);
    }

    public <T extends Event> Subscription registerListener(Class<T> eventType,
                                                           EventListener<? super T> listener) {
        return handlerRegistry.registerListener(eventType, listener);
    }

    public <T extends Event> Subscription registerListener(Class<T> eventType, int priority,
                                                           EventListener<? super T> listener) {
        return handlerRegistry.registerListener(eventType, priority, listener);
    }

    public <T extends Event> Subscription registerListener(final Class<T> eventType, int priority,
                                                           boolean ignoreCancelled,
                                                           final EventListener<? super T> listener) {
        return handlerRegistry.registerListener(eventType, priority, ignoreCancelled, listener);
    }

    public <T extends Event> Subscription registerListenerOnce(Class<T> eventType,
                                                               EventListener<? super T> listener) {
        return handlerRegistry.registerListenerOnce(eventType, listener);
    }

    public <T extends Event> Subscription registerListenerOnce(Class<T> eventType, int priority,
                                                               EventListener<? super T> listener) {
        return handlerRegistry.registerListenerOnce(eventType, priority, listener);
    }

    public <T extends Event> Subscription registerListenerOnce(final Class<T> eventType, int priority,
                                                               boolean ignoreCancelled,
                                                               final EventListener<? super T> listener) {
        return handlerRegistry.registerListenerOnce(eventType, priority, ignoreCancelled, listener);
    }

    @SafeVarargs
    public final <T extends Event> Subscription registerListener(
            EventListener<? super T> listener, Class<? extends T>... eventTypes) {
        return handlerRegistry.registerListener(listener, eventTypes);
    }

    @SafeVarargs
    public final <T extends Event> Subscription registerListener(
            int priority, EventListener<? super T> listener, Class<? extends T>... eventTypes) {
        return handlerRegistry.registerListener(priority, listener, eventTypes);
    }

    @SafeVarargs
    public final <T extends Event> Subscription registerListener(
            int priority, boolean ignoreCancelled, EventListener<? super T> listener,
            Class<? extends T>... eventTypes) {
        return handlerRegistry.registerListener(priority, ignoreCancelled, listener, eventTypes);
    }

    @SafeVarargs
    public final <T extends Event> Subscription register(
            Consumer<? super T> action, Class<? extends T>... eventTypes) {
        return handlerRegistry.register(action, eventTypes);
    }

    @SafeVarargs
    public final <T extends Event> Subscription register(
            int priority, Consumer<? super T> action, Class<? extends T>... eventTypes) {
        return handlerRegistry.register(priority, action, eventTypes);
    }

    @SafeVarargs
    public final <T extends Event> Subscription register(
            int priority, boolean ignoreCancelled, Consumer<? super T> action,
            Class<? extends T>... eventTypes) {
        return handlerRegistry.register(priority, ignoreCancelled, action, eventTypes);
    }

    public void unregister(final Object listener) {
        handlerRegistry.unregister(listener);
    }

    public void unregister(final Object listener, final Class<? extends Event> eventClass) {
        handlerRegistry.unregister(listener, eventClass);
    }

    public void unregister(final Object listener, final Method method) {
        handlerRegistry.unregister(listener, method);
    }

    public void unregister(final Object listener, final Field field) {
        handlerRegistry.unregister(listener, field);
    }

    public void unregister(final Class<?> listenerClass) {
        handlerRegistry.unregister(listenerClass);
    }

    /**
     * Removes handlers owned by the class itself or by instances assignable to it.
     * This is the historical {@link #unregister(Class)} behavior.
     */
    public void unregisterAssignable(final Class<?> listenerClass) {
        handlerRegistry.unregisterAssignable(listenerClass);
    }

    public void unregister(final Class<?> listenerClass, final Class<? extends Event> eventClass) {
        handlerRegistry.unregister(listenerClass, eventClass);
    }

    /** Removes handlers for the exact event type owned by the class or its instances. */
    public void unregisterAssignable(final Class<?> listenerClass,
                                     final Class<? extends Event> eventClass) {
        handlerRegistry.unregisterAssignable(listenerClass, eventClass);
    }

    /** Removes only handlers whose owner is exactly this class or an instance of this class. */
    public void unregisterExact(final Class<?> listenerClass) {
        handlerRegistry.unregisterExact(listenerClass);
    }

    /** Removes only exact-event handlers whose owner is exactly this class or an instance of this class. */
    public void unregisterExact(final Class<?> listenerClass,
                                 final Class<? extends Event> eventClass) {
        handlerRegistry.unregisterExact(listenerClass, eventClass);
    }

    /**
     * Unregisters all handlers whose listener matches the supplied predicate.
     */
    public void unregisterIf(final Predicate<Object> listenerPredicate) {
        handlerRegistry.unregisterIf(listenerPredicate);
    }

    /**
     * Unregisters all handlers registered for the specified event type.
     */
    public void unregisterEventType(final Class<? extends Event> eventType) {
        handlerRegistry.unregisterEventType(eventType);
    }

    public void unregisterAll() {
        handlerRegistry.unregisterAll();
    }

    /**
     * Actively scans all registered event types and purges any handlers whose
     * weak listener targets have been garbage collected.
     *
     * @return the total count of dead handlers purged from the bus
     */
    public int purgeDeadHandlers() {
        return handlerRegistry.purgeDeadHandlers();
    }

    public void removeEntry(Class<? extends Event> eventType) {
        handlerRegistry.removeEntry(eventType);
    }

    public boolean isRegistered(Object listener) {
        return handlerRegistry.isRegistered(listener);
    }

    public boolean isRegistered(Object listener, Class<? extends Event> eventClass) {
        return handlerRegistry.isRegistered(listener, eventClass);
    }

    public boolean isRegistered(Class<?> listenerClass) {
        return handlerRegistry.isRegistered(listenerClass);
    }

    public int listenerCount() {
        return handlerRegistry.listenerCount();
    }

    public int handlerCount() {
        return handlerRegistry.handlerCount();
    }

    /** Returns a stable, read-only snapshot of event types with registered handlers. */
    public Set<Class<? extends Event>> registeredEventTypes() {
        return handlerRegistry.registeredEventTypes();
    }

    /** Returns whether this bus currently has no registered handlers. */
    public boolean isEmpty() {
        return handlerRegistry.isEmpty();
    }

    public int handlerCount(Class<? extends Event> eventType) {
        return handlerRegistry.handlerCount(eventType);
    }

    public int exactHandlerCount(Class<? extends Event> eventType) {
        return handlerRegistry.exactHandlerCount(eventType);
    }

    public boolean hasListeners(Class<? extends Event> eventType) {
        return handlerRegistry.hasListeners(eventType);
    }

    public boolean hasExactListeners(Class<? extends Event> eventType) {
        return handlerRegistry.hasExactListeners(eventType);
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



















































    /**
     * Clears all registrations and derived state: handlers, dispatch caches,
     * mutation stamps, sticky events, upcasters, interceptors and metrics.
     * Configuration (error handler, error policy, thread affinity, parent
     * link) is left untouched. Safe to call repeatedly.
     */
    public void clear() {
        handlerRegistry.clear();
        upcasterRegistry.clear();
        stickyStore.clear();
        interceptorList.clear();
        interceptor = null;
        resetMetrics();
    }














    /**



    // ==========================================
    // Context-Aware Event Dispatching
    // ==========================================




    // ==========================================
    // Sticky Events Support
    // ==========================================


    /**
     * Writes the sticky cache entry for the event's runtime type (including the
     * upcaster cascade) without dispatching it. Package-private so the
     * transaction machinery can replay it at commit time.
     */
    void writeSticky(Event event) {
        stickyStore.write(event);
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
        upcasterRegistry.register(upcaster);
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
        return stickyStore.get(eventType);
    }

    /**
     * Removes and returns the cached sticky event of the specified class.
     *
     * @param eventType the event class
     * @param <T>       the event type
     * @return the removed event, or {@code null}
     */
    public <T extends Event> T removeSticky(Class<T> eventType) {
        return stickyStore.remove(eventType);
    }

    /**
     * Clears all cached sticky events from this event bus.
     */
    public void clearSticky() {
        stickyStore.clear();
    }

    void replaySticky(Class<?> targetType, Type genericType, Handler handler) {
        if (stickyStore.isEmpty()) {
            return;
        }
        for (Event sticky : stickyStore.matching(targetType)) {
            try {
                if (handler.isHandlingEvents(sticky) && handler.matchesGenericEvent(sticky)) {
                    handler.invoke(sticky);
                }
            } catch (Throwable t) {
                dispatchPipeline.handleFailure(sticky, handler.listener, t);
            }
        }
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

    Map<Class<? extends Event>, List<String>> getListenerTopologySnapshot() {
        return handlerRegistry.getListenerTopologySnapshot();
    }

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



    static ScheduledExecutorService getTimeoutScheduler() {
        return TimeoutScheduler.scheduler();
    }


    void reportAsyncFailure(Event event, Throwable throwable) {
        dispatchPipeline.reportAsyncFailure(event, throwable);
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
        return handlerRegistry.registerSubscriber(eventType, priority, ignoreCancelled, once, weak, sticky, filter, genericType, action);
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
        return handlerRegistry.registerSubscriber(eventType, priority, ignoreCancelled, once, weak, sticky, filter, genericType, id, after, before, afterClasses, beforeClasses, action);
    }

    Map<Class<?>, List<Class<?>>> getUpcasterTopologySnapshot() {
        return upcasterRegistry.getUpcasterTopologySnapshot();
    }

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
     * Executes an event transaction with access to the {@link TransactionContext} for Saga compensations.
     *
     * @param action transactional logic
     */
    public void transaction(final Consumer<TransactionContext> action) {
        ensureOpen();
        EventTransactions.executeWithContext(this, action);
    }

    public boolean isTransactionActive() {
        return EventTransactions.current() != null;
    }
    public <T extends Event & Cancellable> boolean dispatchCancelled(T event) {
        return dispatchPipeline.dispatchCancelled(event);
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

    public String toString() {
        return "EventManager[name=\"" + name + "\""
                + ", registeredTypes=" + handlerRegistry.registeredTypeCount()
                + ", handlers=" + handlerRegistry.handlerCount()
                + ", metrics=" + busMetrics.isEnabled()
                + ", closed=" + closed
                + (parent != null ? ", parent=\"" + parent.getName() + "\"" : "")
                + ']';
    }

}
