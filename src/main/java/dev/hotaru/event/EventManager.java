package dev.hotaru.event;

import dev.hotaru.event.annotations.EventTarget;
import dev.hotaru.event.impl.Cancellable;
import dev.hotaru.event.impl.Event;
import dev.hotaru.event.impl.Stoppable;

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
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.ArrayList;
import java.lang.ref.WeakReference;
import java.util.Arrays;
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
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

public class EventManager implements AutoCloseable {
    private static final Logger log = Logger.getLogger(EventManager.class.getName());
    private static final int DEFAULT_PRIORITY = Priority.NORMAL;
    private static final Handler[] NO_HANDLERS = new Handler[0];
    private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();

    private static final Method PRIVATE_LOOKUP_IN = resolvePrivateLookupIn();
    private static final java.lang.reflect.Constructor<MethodHandles.Lookup> JAVA8_LOOKUP_CTOR = resolveJava8LookupConstructor();
    private static final EventErrorHandler DEFAULT_ERROR_HANDLER = new EventErrorHandler() {
        @Override
        public void handle(Event event, Object listener, Throwable throwable) {
            log.log(Level.SEVERE, "Failed to dispatch " + event.getClass().getName()
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
    private static final Comparator<Method> METHOD_SCAN_ORDER = new Comparator<Method>() {
        @Override
        public int compare(Method left, Method right) {
            int result = left.getName().compareTo(right.getName());
            if (result != 0) return result;
            Class<?>[] leftParameters = left.getParameterTypes();
            Class<?>[] rightParameters = right.getParameterTypes();
            result = Integer.compare(leftParameters.length, rightParameters.length);
            for (int i = 0; result == 0 && i < leftParameters.length; i++) {
                result = leftParameters[i].getName().compareTo(rightParameters[i].getName());
            }
            if (result != 0) return result;
            return left.getReturnType().getName().compareTo(right.getReturnType().getName());
        }
    };
    private static final Comparator<Field> FIELD_SCAN_ORDER = new Comparator<Field>() {
        @Override
        public int compare(Field left, Field right) {
            int result = left.getName().compareTo(right.getName());
            return result != 0 ? result : left.getType().getName().compareTo(right.getType().getName());
        }
    };
    private final ConcurrentMap<Class<? extends Event>, Handler[]> eventHandlers =
            new ConcurrentHashMap<Class<? extends Event>, Handler[]>();
    private final ConcurrentMap<Class<?>, CachedDispatch> dispatchCache =
            new ConcurrentHashMap<Class<?>, CachedDispatch>();
    private static final ClassValue<ConcurrentMap<Method, InvokerFactory>> INVOKER_FACTORIES =
            new ClassValue<ConcurrentMap<Method, InvokerFactory>>() {
                @Override
                protected ConcurrentMap<Method, InvokerFactory> computeValue(Class<?> type) {
                    return new ConcurrentHashMap<Method, InvokerFactory>();
                }
            };
    private static final ClassValue<ListenerPlan> LISTENER_PLANS = new ClassValue<ListenerPlan>() {
        @Override
        protected ListenerPlan computeValue(Class<?> type) {
            return buildListenerPlan(type);
        }
    };
    private static final ClassValue<ConcurrentMap<Field, FieldAccessor>> FIELD_ACCESSORS =
            new ClassValue<ConcurrentMap<Field, FieldAccessor>>() {
                @Override
                protected ConcurrentMap<Field, FieldAccessor> computeValue(Class<?> type) {
                    return new ConcurrentHashMap<Field, FieldAccessor>();
                }
            };
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
    private final ConcurrentMap<Class<?>, MetricCounter> metricsByType =
            new ConcurrentHashMap<Class<?>, MetricCounter>();

    private static final ClassValue<EventFilter<Event>> FILTER_CACHE =
            new ClassValue<EventFilter<Event>>() {
                @SuppressWarnings("unchecked")
                @Override
                protected EventFilter<Event> computeValue(Class<?> type) {
                    try {
                        java.lang.reflect.Constructor<?> constructor = type.getDeclaredConstructor();
                        constructor.setAccessible(true);
                        return (EventFilter<Event>) constructor.newInstance();
                    } catch (Throwable t) {
                        throw new IllegalArgumentException("Failed to instantiate EventFilter: " + type, t);
                    }
                }
            };

    @SuppressWarnings("unchecked")
    private static EventFilter<Event> filterFor(Class<? extends EventFilter> filterClass) {
        if (filterClass == null || filterClass == EventFilter.PassAll.class) {
            return null;
        }
        return FILTER_CACHE.get(filterClass);
    }
    private volatile boolean metricsEnabled = true;
    private volatile EventErrorHandler errorHandler = DEFAULT_ERROR_HANDLER;
    private volatile ErrorPolicy errorPolicy = ErrorPolicy.CONTINUE;
    private volatile EventInterceptor interceptor;

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

    public void setErrorHandler(EventErrorHandler errorHandler) {
        this.errorHandler = errorHandler != null ? errorHandler : DEFAULT_ERROR_HANDLER;
    }

    public EventErrorHandler getErrorHandler() {
        return errorHandler;
    }

    public ErrorPolicy getErrorPolicy() {
        return errorPolicy;
    }

    public void setErrorPolicy(ErrorPolicy errorPolicy) {
        this.errorPolicy = errorPolicy != null ? errorPolicy : ErrorPolicy.CONTINUE;
    }

    public EventInterceptor getInterceptor() {
        return interceptor;
    }

    public void setInterceptor(EventInterceptor interceptor) {
        this.interceptor = interceptor;
    }

    /**
     * Appends an interceptor to the current interceptor chain.
     */
    public synchronized void addInterceptor(EventInterceptor interceptor) {
        if (interceptor == null) {
            return;
        }
        this.interceptor = EventInterceptors.chain(this.interceptor, interceptor);
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

    public void setDeadEventsEnabled(boolean deadEventsEnabled) {
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

    public void register(Object listener) {
        register(listener, (Class<? extends Event>) null);
    }

    public void register(Object listener, Class<? extends Event> eventClass) {
        if (listener == null) {
            return;
        }
        if (listener instanceof Consumer<?>) {
            if (eventClass == null) {
                return;
            }
            @SuppressWarnings("unchecked")
            Consumer<Event> consumer = (Consumer<Event>) listener;
            @SuppressWarnings("unchecked")
            Class<Event> evt = (Class<Event>) eventClass;
            register(evt, consumer);
            return;
        }
        if (listener instanceof Class<?>) {
            register((Class<?>) listener, eventClass);
            return;
        }
        bindListenerPlan(listener, listenerPlanFor(listener.getClass()), eventClass, false, false);
    }

    /**
     * Registers an object listener using a weak reference to prevent memory leaks.
     * When the listener is garbage collected, its handlers will automatically deactivate.
     */
    public void registerWeak(Object listener) {
        registerWeak(listener, (Class<? extends Event>) null);
    }

    /**
     * Registers an object listener weakly for a specific event class.
     */
    public void registerWeak(Object listener, Class<? extends Event> eventClass) {
        if (listener == null) {
            return;
        }
        if (listener instanceof Consumer<?>) {
            if (eventClass == null) {
                return;
            }
            @SuppressWarnings("unchecked")
            Consumer<Event> consumer = (Consumer<Event>) listener;
            @SuppressWarnings("unchecked")
            Class<Event> evt = (Class<Event>) eventClass;
            registerWeak(evt, consumer);
            return;
        }
        if (listener instanceof Class<?>) {
            register((Class<?>) listener, eventClass);
            return;
        }
        bindListenerPlan(listener, listenerPlanFor(listener.getClass()), eventClass, false, true);
    }

    public void register(Class<?> listenerClass) {
        register(listenerClass, (Class<? extends Event>) null);
    }

    public void register(Class<?> listenerClass, Class<? extends Event> eventClass) {
        if (listenerClass == null) {
            return;
        }
        bindListenerPlan(listenerClass, listenerPlanFor(listenerClass), eventClass, true);
    }

    public void register(Object listener, Method method) {
        if (listener == null || method == null) {
            return;
        }
        bindMatchingDefinitions(listener, method, null, listener instanceof Class<?>);
    }

    public void register(Object listener, Field field) {
        if (listener == null || field == null) {
            return;
        }
        bindMatchingDefinitions(listener, null, field, listener instanceof Class<?>);
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

    public <T extends Event> Subscription registerWeak(Class<T> eventType, Consumer<? super T> action) {
        return registerWeak(eventType, DEFAULT_PRIORITY, false, action);
    }

    public <T extends Event> Subscription registerWeak(Class<T> eventType, int priority, Consumer<? super T> action) {
        return registerWeak(eventType, priority, false, action);
    }

    public <T extends Event> Subscription registerWeak(final Class<T> eventType, int priority,
                                                       boolean ignoreCancelled, final Consumer<? super T> action) {
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
                normalizePriority(priority),
                ignoreCancelled,
                registrationOrder.getAndIncrement(),
                false,
                null,
                new Invoker() {
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

    public Subscription subscribe(Object listener) {
        if (listener == null) {
            return Subscription.NOOP;
        }
        if (listener instanceof Class<?>) {
            return subscribe((Class<?>) listener);
        }
        synchronized (this) {
            if (isRegistered(listener)) {
                return Subscription.NOOP;
            }
            register(listener);
            return ownedSubscription(handlersForListener(listener, null));
        }
    }

    public Subscription subscribe(Class<?> listenerClass) {
        if (listenerClass == null) {
            return Subscription.NOOP;
        }
        synchronized (this) {
            if (isRegistered(listenerClass)) {
                return Subscription.NOOP;
            }
            register(listenerClass);
            return ownedSubscription(handlersForListener(listenerClass, null));
        }
    }

    public Subscription subscribe(Object listener, Class<? extends Event> eventClass) {
        if (listener == null || eventClass == null) {
            return Subscription.NOOP;
        }
        if (listener instanceof Consumer<?>) {
            @SuppressWarnings("unchecked")
            Consumer<Event> consumer = (Consumer<Event>) listener;
            @SuppressWarnings("unchecked")
            Class<Event> evt = (Class<Event>) eventClass;
            return register(evt, consumer);
        }
        synchronized (this) {
            if (isRegistered(listener, eventClass)) {
                return Subscription.NOOP;
            }
            if (listener instanceof Class<?>) {
                register((Class<?>) listener, eventClass);
            } else {
                register(listener, eventClass);
            }
            return ownedSubscription(handlersForListener(listener, eventClass));
        }
    }

    /**
     * Subscribes an object listener weakly, returning a subscription.
     */
    public Subscription subscribeWeak(Object listener) {
        if (listener == null) {
            return Subscription.NOOP;
        }
        if (listener instanceof Class<?>) {
            return subscribe((Class<?>) listener);
        }
        synchronized (this) {
            if (isRegistered(listener)) {
                return Subscription.NOOP;
            }
            registerWeak(listener);
            return ownedSubscription(handlersForListener(listener, null));
        }
    }

    /**
     * Subscribes an object listener weakly for a specific event class.
     */
    public Subscription subscribeWeak(Object listener, Class<? extends Event> eventClass) {
        if (listener == null || eventClass == null) {
            return Subscription.NOOP;
        }
        if (listener instanceof Consumer<?>) {
            @SuppressWarnings("unchecked")
            Consumer<Event> consumer = (Consumer<Event>) listener;
            @SuppressWarnings("unchecked")
            Class<Event> evt = (Class<Event>) eventClass;
            return registerWeak(evt, consumer);
        }
        synchronized (this) {
            if (isRegistered(listener, eventClass)) {
                return Subscription.NOOP;
            }
            if (listener instanceof Class<?>) {
                register((Class<?>) listener, eventClass);
            } else {
                registerWeak(listener, eventClass);
            }
            return ownedSubscription(handlersForListener(listener, eventClass));
        }
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
        for (Class<? extends Event> eventType : eventHandlers.keySet()) {
            removeHandlers(eventType, new Predicate<Handler>() {
                @Override
                public boolean test(Handler handler) {
                    if (handler.isWeak() && handler.isDead()) {
                        purged.incrementAndGet();
                        return true;
                    }
                    return false;
                }
            });
        }
        return purged.get();
    }

    public void clear() {
        boolean hadRegistrations = !eventHandlers.isEmpty() || !dispatchCache.isEmpty();
        for (Handler[] handlers : eventHandlers.values()) {
            for (Handler handler : handlers) {
                handler.active = false;
            }
        }
        eventHandlers.clear();
        dispatchCache.clear();
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
                handler.active = false;
            }
            dispatchCache.clear();
            mutationVersion.incrementAndGet();
        }
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
                if (handlers[i].matchesListener(listener)) {
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
                if (handler.listener == listenerClass
                        || listenerClass.isAssignableFrom(listenerClassOf(handler.listener))) {
                    return true;
                }
            }
            return false;
        }
        for (int i = 0; i < handlers.length; i++) {
            if (handlers[i].matchesListener(listener)) {
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
                if (handler.listener == listenerClass
                        || listenerClass.isAssignableFrom(listenerClassOf(handler.listener))) {
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
                listeners.add(handlers[i].listener);
            }
        }
        return listeners.size();
    }

    public int handlerCount() {
        int count = 0;
        for (Handler[] handlers : eventHandlers.values()) {
            count += handlers.length;
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
        return eventType != null && handlersFor(eventType).length != 0;
    }

    public boolean hasExactListeners(Class<? extends Event> eventType) {
        return eventType != null && exactHandlersFor(eventType).length != 0;
    }

    public <T extends Event> T call(T event) {
        if (event == null || closed) {
            return event;
        }

        checkThreadAffinity(event);

        long startNanos = metricsEnabled ? System.nanoTime() : 0L;
        if (metricsEnabled) {
            dispatchedEvents.increment();
            recordDispatch(event.getClass());
        }

        Handler[] handlers = handlersFor(event.getClass());
        if (handlers.length == 0) {
            if (metricsEnabled) {
                recordDuration(event.getClass(), System.nanoTime() - startNanos);
            }
            if (parent != null) {
                parent.call(event);
            } else if (deadEventsEnabled && !(event instanceof DeadEvent) && hasListeners(DeadEvent.class)) {
                call(new DeadEvent(this, event));
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

        if (parent != null) {
            boolean canBubble = true;
            if (event instanceof Cancellable && ((Cancellable) event).isCancelled()) {
                canBubble = false;
            }
            if (event instanceof Stoppable && ((Stoppable) event).isStopped()) {
                canBubble = false;
            }
            if (canBubble) {
                parent.call(event);
            }
        }

        return event;
    }

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

    public <T extends Event> T callExact(T event) {
        if (event == null || closed) {
            return event;
        }

        checkThreadAffinity(event);

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
            if (metricsEnabled) {
                recordDuration(event.getClass(), System.nanoTime() - startNanos);
            }
            if (parent != null) {
                parent.callExact(event);
            } else if (deadEventsEnabled && !(event instanceof DeadEvent) && hasListeners(DeadEvent.class)) {
                call(new DeadEvent(this, event));
            }
        }
        return event;
    }

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
     * Dispatches a cancellable event and returns whether it was cancelled.
     */
    public <T extends Event & Cancellable> boolean callCancelled(T event) {
        if (event == null) {
            return false;
        }
        call(event);
        return event.isCancelled();
    }

    /**
     * Dispatches multiple events in sequential order.
     */
    public void callAll(Event... events) {
        if (events == null || events.length == 0) {
            return;
        }
        for (int i = 0; i < events.length; i++) {
            Event event = events[i];
            if (event != null) {
                call(event);
            }
        }
    }

    /**
     * Dispatches an iterable collection of events in sequential order.
     */
    public void callAll(Iterable<? extends Event> events) {
        if (events == null) {
            return;
        }
        for (Event event : events) {
            if (event != null) {
                call(event);
            }
        }
    }

    /**
     * Dispatches an event asynchronously using ForkJoinPool.commonPool().
     */
    public <T extends Event> CompletableFuture<T> callAsync(final T event) {
        return callAsync(event, this.defaultExecutor);
    }

    /**
     * Dispatches an event through the supplied executor. The returned future
     * completes with the same event instance after dispatch finishes.
     */
    public <T extends Event> CompletableFuture<T> callAsync(final T event, Executor executor) {
        return submit(event, executor, false);
    }

    /**
     * Dispatches an event to exact-type handlers asynchronously using ForkJoinPool.commonPool().
     */
    public <T extends Event> CompletableFuture<T> callExactAsync(final T event) {
        return callExactAsync(event, this.defaultExecutor);
    }

    /**
     * Dispatches an event to exact-type handlers through the supplied executor.
     */
    public <T extends Event> CompletableFuture<T> callExactAsync(final T event, Executor executor) {
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
        try {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        future.complete(exact ? callExact(event) : call(event));
                    } catch (Throwable t) {
                        future.completeExceptionally(t);
                    }
                }
            });
        } catch (Throwable submissionFailure) {
            future.completeExceptionally(submissionFailure);
        }
        return future;
    }

    public <T extends Event> T call(Class<T> eventType, Supplier<T> supplier) {
        if (eventType == null || supplier == null || !hasListeners(eventType)) {
            return null;
        }
        return call(supplier.get());
    }

    public <T extends Event> T callExact(Class<T> eventType, Supplier<T> supplier) {
        if (eventType == null || supplier == null || !hasExactListeners(eventType)) {
            return null;
        }
        return callExact(supplier.get());
    }

    private static final class DispatchFrame implements Runnable {
        private EventManager bus;
        private Event event;
        private Handler[] handlers;
        private DispatchFrame next;

        private void init(EventManager bus, Event event, Handler[] handlers) {
            this.bus = bus;
            this.event = event;
            this.handlers = handlers;
        }

        private void clear() {
            this.bus = null;
            this.event = null;
            this.handlers = null;
        }

        @Override
        public void run() {
            EventManager targetBus = this.bus;
            if (targetBus != null) {
                targetBus.doDispatch(event, handlers);
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
            frame.clear();
            frame.next = head;
            head = frame;
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
                pool.release(frame);
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

        for (int i = 0; i < handlers.length; i++) {
            Handler handler = handlers[i];

            // A dispatch snapshot may outlive a concurrent unregister/clear.
            if (!handler.active) {
                continue;
            }

            if (stoppable != null && stoppableReadable) {
                boolean stopped;
                try {
                    stopped = stoppable.isStopped();
                } catch (Throwable t) {
                    if (!handleFailure(event, event, t)) return;
                    // Under CONTINUE, treat an unreadable state as false for
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
                handling = handler.isHandlingEvents(event);
            } catch (Throwable t) {
                if (!handleFailure(event, handler.listener, t)) return;
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
                    if (!handleFailure(event, event, t)) return;
                    // Under CONTINUE, treat an unreadable state as false for
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
                    if (!handleFailure(event, handler.listener, t)) return;
                    continue;
                }
                if (!accepted) {
                    continue;
                }
            }

            try {
                if (handler.once) {
                    handler.active = false;
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
                if (!handleFailure(event, handler.listener, t)) return;
            }
        }
    }

    private boolean handleFailure(Event event, Object source, Throwable throwable) {
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
        return policy == ErrorPolicy.CONTINUE;
    }

    private MetricCounter counterFor(Class<?> eventType) {
        if (eventType == null) {
            return null;
        }
        MetricCounter counter = metricsByType.get(eventType);
        if (counter == null) {
            MetricCounter created = new MetricCounter();
            MetricCounter previous = metricsByType.putIfAbsent(eventType, created);
            counter = previous != null ? previous : created;
        }
        return counter;
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
        handlers.sort(HANDLER_ORDER);
        return handlers.toArray(NO_HANDLERS);
    }

    private static ListenerPlan listenerPlanFor(final Class<?> listenerClass) {
        return LISTENER_PLANS.get(listenerClass);
    }

    private static ListenerPlan buildListenerPlan(Class<?> listenerClass) {
        List<HandlerDefinition> definitions = new ArrayList<HandlerDefinition>();
        scanListenerType(listenerClass, definitions, new HashSet<Class<?>>(),
                new java.util.HashMap<MethodSignature, List<Method>>(), listenerClass);
        return new ListenerPlan(definitions.toArray(new HandlerDefinition[definitions.size()]));
    }

    private static void scanListenerType(Class<?> type, List<HandlerDefinition> definitions,
                                   Set<Class<?>> visitedTypes,
                                   Map<MethodSignature, List<Method>> seenMethods,
                                   Class<?> concreteListenerClass) {
        if (type == null || type == Object.class || !visitedTypes.add(type)) {
            return;
        }

        Method[] declaredMethods = type.getDeclaredMethods();
        Arrays.sort(declaredMethods, METHOD_SCAN_ORDER);
        for (Method method : declaredMethods) {
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

            if (shadowed || !method.isAnnotationPresent(EventTarget.class)) {
                continue;
            }
            if (method.getParameterTypes().length != 1) {
                log.warning("Skipping handler " + method + " because it must have exactly one parameter");
                continue;
            }
            if (method.getReturnType() != Void.TYPE) {
                log.warning("Skipping handler " + method + " because handler methods must return void");
                continue;
            }

            Class<?> parameterType = method.getParameterTypes()[0];
            if (!Event.class.isAssignableFrom(parameterType)) {
                log.warning("Skipping handler " + method + " because parameter type is not an Event");
                continue;
            }

            EventTarget eventTarget = method.getAnnotation(EventTarget.class);
            int priority = annotationPriority(eventTarget, DEFAULT_PRIORITY);
            final EventFilter<Event> eventFilter = filterFor(eventTarget.filter());
            Predicate<Event> filter = eventFilter == null ? null : new Predicate<Event>() {
                @Override
                public boolean test(Event e) {
                    return eventFilter.test(e);
                }
            };
            definitions.add(HandlerDefinition.forMethod(
                    method,
                    parameterType.asSubclass(Event.class),
                    priority,
                    eventTarget.ignoreCancelled(),
                    filter
            ));
        }

        Field[] declaredFields = type.getDeclaredFields();
        Arrays.sort(declaredFields, FIELD_SCAN_ORDER);
        for (Field field : declaredFields) {
            if (field.isSynthetic() || !field.isAnnotationPresent(EventTarget.class)) {
                continue;
            }
            if (!EventListener.class.isAssignableFrom(field.getType())) {
                log.warning("Skipping listener field " + field + " because it is not an EventListener");
                continue;
            }

            Class<? extends Event> fieldEventType = eventTypeFromField(field, concreteListenerClass);
            if (fieldEventType == null) {
                log.warning("Skipping listener field " + field
                        + " because its EventListener event type could not be inferred");
                continue;
            }

            EventTarget eventTarget = field.getAnnotation(EventTarget.class);
            boolean listenerPriority = eventTarget.value() == Priority.UNSPECIFIED;
            int priority = annotationPriority(eventTarget, DEFAULT_PRIORITY);
            final EventFilter<Event> eventFilter = filterFor(eventTarget.filter());
            Predicate<Event> filter = eventFilter == null ? null : new Predicate<Event>() {
                @Override
                public boolean test(Event e) {
                    return eventFilter.test(e);
                }
            };
            definitions.add(HandlerDefinition.forField(
                    field,
                    fieldEventType,
                    priority,
                    listenerPriority,
                    eventTarget.ignoreCancelled(),
                    filter
            ));
        }

        Class<?>[] interfaces = type.getInterfaces();
        Arrays.sort(interfaces, new Comparator<Class<?>>() {
            @Override
            public int compare(Class<?> left, Class<?> right) {
                return left.getName().compareTo(right.getName());
            }
        });
        for (Class<?> iface : interfaces) {
            scanListenerType(iface, definitions, visitedTypes, seenMethods, concreteListenerClass);
        }
        scanListenerType(type.getSuperclass(), definitions, visitedTypes, seenMethods, concreteListenerClass);
    }

    private static int annotationPriority(EventTarget eventTarget, int fallback) {
        return eventTarget.value() != Priority.UNSPECIFIED ? eventTarget.value() : fallback;
    }

    private static int normalizePriority(int priority) {
        return priority == Priority.UNSPECIFIED ? DEFAULT_PRIORITY : priority;
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

    private void bindMatchingDefinitions(Object listener, Method method, Field field, boolean staticOnly) {
        ListenerPlan plan = listenerPlanFor(listener instanceof Class<?>
                ? (Class<?>) listener
                : listener.getClass());
        for (int i = 0; i < plan.definitions.length; i++) {
            HandlerDefinition definition = plan.definitions[i];
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

    private void bindListenerPlan(Object listener, ListenerPlan plan, Class<? extends Event> eventClass,
                                  boolean staticOnly) {
        bindListenerPlan(listener, plan, eventClass, staticOnly, false);
    }

    private void bindListenerPlan(Object listener, ListenerPlan plan, Class<? extends Event> eventClass,
                                  boolean staticOnly, boolean weak) {
        for (HandlerDefinition definition : plan.definitions) {
            if (eventClass != null && definition.eventType != eventClass) {
                continue;
            }
            bindDefinition(listener, definition, staticOnly, weak);
        }
    }

    private void bindDefinition(final Object listener, final HandlerDefinition definition, boolean staticOnly) {
        bindDefinition(listener, definition, staticOnly, false);
    }

    private void bindDefinition(final Object listener, final HandlerDefinition definition, boolean staticOnly, final boolean weak) {
        if (staticOnly && !definition.staticMember) {
            return;
        }

        if (definition.method != null) {
            Method method = definition.method;
            Invoker invoker;
            if (weak && !definition.staticMember) {
                final WeakReference<Object> weakRef = new WeakReference<Object>(listener);
                final InvokerFactory factory = invokerFactoryFor(method);
                invoker = new Invoker() {
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
                    invoker
            );
            addHandler(handler);
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

        final Invoker invoker;
        final WeakReference<Object> weakRef = (weak && !definition.staticMember) ? new WeakReference<Object>(listener) : null;
        if (Modifier.isFinal(field.getModifiers())) {
            @SuppressWarnings("unchecked")
            final EventListener<Event> directListener = (EventListener<Event>) fieldListener;
            if (weakRef != null) {
                invoker = new Invoker() {
                    @Override
                    public void invoke(Event event) {
                        if (weakRef.get() != null) {
                            directListener.onEvent(dispatchType.cast(event));
                        }
                    }
                };
            } else {
                invoker = new Invoker() {
                    @Override
                    public void invoke(Event event) {
                        directListener.onEvent(dispatchType.cast(event));
                    }
                };
            }
        } else {
            if (weakRef != null) {
                invoker = new Invoker() {
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
                invoker = new Invoker() {
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
                invoker
        );
        addHandler(handler);
    }

    private void addHandler(final Handler handler) {
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
            // Registration invalidates every flattened hierarchy cache. Clearing
            // eagerly prevents stale Class keys from accumulating when callers
            // create many short-lived event classes.
            dispatchCache.clear();
            mutationVersion.incrementAndGet();
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
            dispatchCache.clear();
            mutationVersion.incrementAndGet();
        }
    }

    private void removeHandlers(final Predicate<Handler> predicate) {
        boolean removed = false;
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
                if (updated.length == 0) {
                    if (eventHandlers.remove(eventType, handlers)) {
                        removed = true;
                        markInactive(handlers, predicate);
                        break;
                    }
                } else {
                    if (eventHandlers.replace(eventType, handlers, updated)) {
                        removed = true;
                        markInactive(handlers, predicate);
                        break;
                    }
                }
            }
        }
        if (removed) {
            dispatchCache.clear();
            mutationVersion.incrementAndGet();
        }
    }

    private static void markInactive(Handler[] handlers, Predicate<Handler> predicate) {
        for (int i = 0; i < handlers.length; i++) {
            if (predicate.test(handlers[i])) {
                handlers[i].active = false;
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

    private EventListener<?> listenerFromField(Object listener, Field field, boolean reportFailure) {
        try {
            Object value = fieldAccessor(field).get(Modifier.isStatic(field.getModifiers()) ? null : listener);
            if (value == null) {
                if (reportFailure) {
                    log.warning("Skipping listener field " + field + " because its value is null");
                }
                return null;
            }
            if (!(value instanceof EventListener<?>)) {
                if (reportFailure) {
                    log.warning("Skipping listener field " + field
                            + " because its value is not an EventListener");
                }
                return null;
            }
            return (EventListener<?>) value;
        } catch (IllegalAccessException e) {
            if (reportFailure) {
                log.log(Level.WARNING, "Skipping listener field " + field
                        + " because it is not accessible", e);
            }
            return null;
        } catch (RuntimeException e) {
            if (reportFailure) {
                log.log(Level.WARNING, "Skipping listener field " + field
                        + " because it could not be read", e);
            }
            return null;
        } catch (Throwable e) {
            if (reportFailure) {
                log.log(Level.WARNING, "Skipping listener field " + field
                        + " because it could not be read", e);
            }
            return null;
        }
    }

    private static FieldAccessor fieldAccessor(final Field field) {
        ConcurrentMap<Field, FieldAccessor> accessors = FIELD_ACCESSORS.get(field.getDeclaringClass());
        FieldAccessor accessor = accessors.get(field);
        if (accessor == null) {
            FieldAccessor created = buildFieldAccessor(field);
            FieldAccessor previous = accessors.putIfAbsent(field, created);
            accessor = previous != null ? previous : created;
        }
        return accessor;
    }

    private static FieldAccessor buildFieldAccessor(final Field field) {
        final boolean isStatic = Modifier.isStatic(field.getModifiers());
        boolean wasAccessible = field.isAccessible();
        try {
            try {
                field.setAccessible(true);
            } catch (SecurityException ignored) {
                // The lookup or reflective fallback may still be able to access it.
            } catch (RuntimeException inaccessible) {
                // Strongly encapsulated modules can reject this; try the lookup below.
            }

            MethodHandles.Lookup lookup = lookupFor(field.getDeclaringClass());
            MethodHandle getter = lookup.unreflectGetter(field);
            if (isStatic) {
                final MethodHandle adapted = getter.asType(MethodType.methodType(Object.class));
                return new FieldAccessor() {
                    @Override
                    public Object get(Object owner) throws Throwable {
                        return (Object) adapted.invokeExact();
                    }
                };
            }
            final MethodHandle adapted = getter.asType(MethodType.methodType(Object.class, Object.class));
            return new FieldAccessor() {
                @Override
                public Object get(Object owner) throws Throwable {
                    return (Object) adapted.invokeExact(owner);
                }
            };
        } catch (Throwable lookupFailure) {
            return new FieldAccessor() {
                @Override
                public Object get(Object owner) throws IllegalAccessException {
                    return field.get(isStatic ? null : owner);
                }
            };
        } finally {
            try {
                field.setAccessible(wasAccessible);
            } catch (Throwable ignored) {
                // Best effort: the accessor is already built and no longer needs this flag.
            }
        }
    }

    private static Class<? extends Event> eventTypeFromField(Field field, Class<?> listenerClass) {
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
        eventType = resolveType(eventType, typeArgumentsFor(listenerClass, field.getDeclaringClass()));
        Class<?> eventClass = classFromType(eventType);
        if (eventClass == null || !Event.class.isAssignableFrom(eventClass)) {
            return null;
        }
        return eventClass.asSubclass(Event.class);
    }

    private static Map<TypeVariable<?>, Type> typeArgumentsFor(Class<?> concreteType, Class<?> targetType) {
        Map<TypeVariable<?>, Type> resolved = findTypeArguments(concreteType, targetType,
                new java.util.HashMap<TypeVariable<?>, Type>(), new HashSet<Class<?>>());
        return resolved == null
                ? Collections.<TypeVariable<?>, Type>emptyMap()
                : resolved;
    }

    private static Map<TypeVariable<?>, Type> findTypeArguments(Type currentType, Class<?> targetType,
                                                                 Map<TypeVariable<?>, Type> inherited,
                                                                 Set<Class<?>> visited) {
        Class<?> currentClass;
        Map<TypeVariable<?>, Type> currentArguments = new java.util.HashMap<TypeVariable<?>, Type>(inherited);
        if (currentType instanceof ParameterizedType) {
            ParameterizedType parameterized = (ParameterizedType) currentType;
            if (!(parameterized.getRawType() instanceof Class<?>)) {
                return null;
            }
            currentClass = (Class<?>) parameterized.getRawType();
            TypeVariable<?>[] variables = currentClass.getTypeParameters();
            Type[] actuals = parameterized.getActualTypeArguments();
            for (int i = 0; i < variables.length; i++) {
                currentArguments.put(variables[i], resolveType(actuals[i], inherited));
            }
        } else if (currentType instanceof Class<?>) {
            currentClass = (Class<?>) currentType;
        } else {
            return null;
        }

        if (currentClass == targetType) {
            return currentArguments;
        }
        if (!visited.add(currentClass)) {
            return null;
        }

        for (Type iface : currentClass.getGenericInterfaces()) {
            Map<TypeVariable<?>, Type> result = findTypeArguments(iface, targetType,
                    currentArguments, new HashSet<Class<?>>(visited));
            if (result != null) {
                return result;
            }
        }
        Type superclass = currentClass.getGenericSuperclass();
        if (superclass != null) {
            return findTypeArguments(superclass, targetType, currentArguments,
                    new HashSet<Class<?>>(visited));
        }
        return null;
    }

    private static Type resolveType(Type type, Map<TypeVariable<?>, Type> mappings) {
        Type resolved = type;
        Set<Type> seen = Collections.newSetFromMap(new IdentityHashMap<Type, Boolean>());
        while (resolved instanceof TypeVariable<?> && seen.add(resolved)) {
            Type replacement = mappings.get(resolved);
            if (replacement == null) {
                break;
            }
            resolved = replacement;
        }
        return resolved;
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

        int index = 0;
        while (index < handlers.length && HANDLER_ORDER.compare(handlers[index], handler) <= 0) {
            index++;
        }

        Handler[] updated = new Handler[handlers.length + 1];
        System.arraycopy(handlers, 0, updated, 0, index);
        updated[index] = handler;
        System.arraycopy(handlers, index, updated, index + 1, handlers.length - index);
        return updated;
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

    private static InvokerFactory invokerFactoryFor(Method method) {
        ConcurrentMap<Method, InvokerFactory> factories = INVOKER_FACTORIES.get(method.getDeclaringClass());
        InvokerFactory factory = factories.get(method);
        if (factory == null) {
            InvokerFactory created = buildInvokerFactory(method);
            InvokerFactory previous = factories.putIfAbsent(method, created);
            factory = previous != null ? previous : created;
        }
        return factory;
    }

    private Invoker invokerFor(Method method, Object listener) {
        return invokerFactoryFor(method).create(listener);
    }

    private static InvokerFactory buildInvokerFactory(final Method method) {
        final boolean isStatic = Modifier.isStatic(method.getModifiers());

        try {
            method.setAccessible(true);
        } catch (SecurityException ignored) {

        } catch (RuntimeException inaccessible) {
            // Method-handle and reflection fallbacks below may still work.
        }

        MethodHandles.Lookup lookup = lookupFor(method.getDeclaringClass());

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

        }

        try {
            final MethodHandle handle = lookup.unreflect(method);
            if (isStatic) {
                final MethodHandle adapted = handle.asType(MethodType.methodType(void.class, Event.class));
                final Invoker invoker = new Invoker() {
                    @Override
                    public void invoke(Event event) throws Throwable {
                        adapted.invokeExact(event);
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
                    final MethodHandle bound = handle.bindTo(listener)
                            .asType(MethodType.methodType(void.class, Event.class));
                    return new Invoker() {
                        @Override
                        public void invoke(Event event) throws Throwable {
                            bound.invokeExact(event);
                        }
                    };
                }
            };
        } catch (IllegalAccessException ignored) {

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

    @SuppressWarnings("unchecked")
    private static java.lang.reflect.Constructor<MethodHandles.Lookup> resolveJava8LookupConstructor() {
        try {
            java.lang.reflect.Constructor<MethodHandles.Lookup> ctor =
                    MethodHandles.Lookup.class.getDeclaredConstructor(Class.class, int.class);
            ctor.setAccessible(true);
            return ctor;
        } catch (Throwable ignored) {
            return null;
        }
    }

    private static MethodHandles.Lookup lookupFor(Class<?> declaringClass) {
        if (PRIVATE_LOOKUP_IN != null) {
            try {
                return (MethodHandles.Lookup) PRIVATE_LOOKUP_IN.invoke(null, declaringClass, LOOKUP);
            } catch (Throwable ignored) {
            }
        }
        if (JAVA8_LOOKUP_CTOR != null) {
            try {
                return JAVA8_LOOKUP_CTOR.newInstance(declaringClass, -1);
            } catch (Throwable ignored) {
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

    private interface FieldAccessor {
        Object get(Object owner) throws Throwable;
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
            handler.active = false;
            removeHandlers(handler.eventType, new Predicate<Handler>() {
                @Override
                public boolean test(Handler candidate) {
                    return candidate == handler;
                }
            });
        }

        @Override
        public boolean isSubscribed() {
            if (!subscribed.get() || !handler.active) {
                return false;
            }
            if (handler.weakListener != null && handler.weakListener.get() == null) {
                handler.active = false;
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
                if (!handler.active) {
                    continue;
                }
                if (handler.weakListener != null && handler.weakListener.get() == null) {
                    handler.active = false;
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
        private final Predicate<Event> filter;

        private HandlerDefinition(Method method, Field field, Class<? extends Event> eventType,
                                  int priority, boolean listenerPriority, boolean ignoreCancelled,
                                  boolean staticMember, Predicate<Event> filter) {
            this.method = method;
            this.field = field;
            this.eventType = eventType;
            this.priority = priority;
            this.listenerPriority = listenerPriority;
            this.ignoreCancelled = ignoreCancelled;
            this.staticMember = staticMember;
            this.filter = filter;
        }

        private static HandlerDefinition forMethod(Method method, Class<? extends Event> eventType,
                                                   int priority, boolean ignoreCancelled, Predicate<Event> filter) {
            return new HandlerDefinition(
                    method,
                    null,
                    eventType,
                    priority,
                    false,
                    ignoreCancelled,
                    Modifier.isStatic(method.getModifiers()),
                    filter
            );
        }

        private static HandlerDefinition forField(Field field, Class<? extends Event> eventType,
                                                  int priority, boolean listenerPriority,
                                                  boolean ignoreCancelled, Predicate<Event> filter) {
            return new HandlerDefinition(
                    null,
                    field,
                    eventType,
                    priority,
                    listenerPriority,
                    ignoreCancelled,
                    Modifier.isStatic(field.getModifiers()),
                    filter
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
        private final WeakReference<Object> weakListener;
        private final Method method;
        private final Field field;
        private final Object dedupeOwner;
        private final Class<? extends Event> eventType;
        private final int priority;
        private final boolean ignoreCancelled;
        private final long order;
        private final boolean once;
        private final Predicate<Event> filter;
        private final Invoker invoker;
        private final EventSubscriber subscriber;
        private volatile boolean active = true;

        private Handler(Object listener, boolean weak, Method method, Field field, Object dedupeOwner,
                        Class<? extends Event> eventType, int priority, boolean ignoreCancelled,
                        long order, boolean once, Predicate<Event> filter, Invoker invoker) {
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
            this.invoker = invoker;
            this.subscriber = (listener instanceof EventSubscriber && !weak) ? (EventSubscriber) listener : null;
        }

        private Handler(Object listener, Method method, Field field, Object dedupeOwner,
                        Class<? extends Event> eventType, int priority, boolean ignoreCancelled,
                        long order, boolean once, Predicate<Event> filter, Invoker invoker) {
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
                    active = false;
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

    private static final class MethodSignature {
        private final String name;
        private final Class<?>[] parameterTypes;

        private MethodSignature(Method method) {
            this.name = method.getName();
            this.parameterTypes = method.getParameterTypes();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof MethodSignature)) return false;
            MethodSignature that = (MethodSignature) o;
            return Objects.equals(name, that.name) && Arrays.equals(parameterTypes, that.parameterTypes);
        }

        @Override
        public int hashCode() {
            return 31 * Objects.hashCode(name) + Arrays.hashCode(parameterTypes);
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

    public static final class Builder {
        private String name = "EventManager";
        private Executor defaultExecutor;
        private ErrorPolicy errorPolicy = ErrorPolicy.CONTINUE;
        private EventErrorHandler errorHandler = DEFAULT_ERROR_HANDLER;
        private boolean metricsEnabled = true;
        private boolean deadEventsEnabled = true;
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
            EventManager bus = new EventManager(this.name, null, this.errorHandler, this.errorPolicy, this.defaultExecutor);
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
