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
 *     <li>Annotated {@link EventListener} fields are supported for reusable typed listeners.</li>
 *     <li>Static-only registration of utility classes via {@link #register(Class)} - no instance needed.</li>
 *     <li>Handlers are discovered across the listener's whole class hierarchy (superclasses and interfaces).</li>
 *     <li>Events are dispatched to handlers registered for any supertype of the event
 *     (superclasses and interfaces), with the flattened dispatch array cached per event class.</li>
 *     <li>Static and private handler methods are supported.</li>
 *     <li>Invocation goes through {@link LambdaMetafactory} when possible (using a private lookup on
 *     Java 9+ so even private handlers take the fast path), falling back to {@link MethodHandle} and
 *     finally plain reflection. Invoker factories are cached per method, so repeatedly registering and
 *     unregistering the same listener class or component is cheap.</li>
 *     <li>{@link Cancellable} events can skip handlers marked {@code ignoreCancelled};
 *     {@link Stoppable} events abort dispatch entirely.</li>
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
        this.registrationOrder = new AtomicLong();
        this.mutationVersion = new AtomicLong();
        this.errorHandler = errorHandler != null ? errorHandler : DEFAULT_ERROR_HANDLER;
    }

    /**
     * Replaces the handler invoked when a listener throws. Passing {@code null} restores the default
     * (logging) behaviour.
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
     */
    public void register(Object listener, Class<? extends Event> eventClass) {
        if (listener == null) {
            return;
        }

        Set<Class<?>> visitedTypes = new HashSet<Class<?>>();
        Set<MethodSignature> seenSignatures = new HashSet<MethodSignature>();
        scanType(listener, listener.getClass(), eventClass, visitedTypes, seenSignatures, false);
    }

    /**
     * Registers the <em>static</em> {@link EventTarget} methods and fields of {@code listenerClass} without needing
     * an instance. Undo with {@link #unregister(Class)}.
     */
    public void register(Class<?> listenerClass) {
        if (listenerClass == null) {
            return;
        }

        Set<Class<?>> visitedTypes = new HashSet<Class<?>>();
        Set<MethodSignature> seenSignatures = new HashSet<MethodSignature>();
        scanType(listenerClass, listenerClass, null, visitedTypes, seenSignatures, true);
    }

    /**
     * Registers a functional listener for events of exactly {@code eventType} (and its subtypes,
     * through normal hierarchy dispatch) at {@link Priority#NORMAL}.
     *
     * @return a subscription used to remove this listener again
     */
    public <T extends Event> Subscription register(Class<T> eventType, Consumer<? super T> action) {
        return register(eventType, DEFAULT_PRIORITY, false, action);
    }

    /**
     * Registers a functional listener with an explicit priority (lower runs first).
     */
    public <T extends Event> Subscription register(Class<T> eventType, int priority, Consumer<? super T> action) {
        return register(eventType, priority, false, action);
    }

    /**
     * Registers a functional listener with an explicit priority and cancellation behaviour.
     *
     * @param ignoreCancelled if true, the listener is skipped once a cancellable event has been cancelled
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

        return new Subscription() {
            @Override
            public void unsubscribe() {
                removeHandlers(new Predicate<Handler>() {
                    @Override
                    public boolean test(Handler candidate) {
                        return candidate == handler;
                    }
                });
            }
        };
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
     * @return number of handlers that would receive an event of {@code eventType}
     */
    public int handlerCount(Class<? extends Event> eventType) {
        return eventType == null ? 0 : handlersFor(eventType).length;
    }

    /**
     * @return true if at least one handler would receive an event of the given type
     */
    public boolean hasListeners(Class<? extends Event> eventType) {
        return eventType != null && handlersFor(eventType).length != 0;
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
     * Dispatches an event. Alias for {@link #call(Event)}.
     */
    public <T extends Event> T post(T event) {
        return call(event);
    }

    /**
     * Dispatches an event. Alias for {@link #call(Event)}.
     */
    public <T extends Event> T fire(T event) {
        return call(event);
    }

    /**
     * Lazily constructs and dispatches an event: {@code supplier} is only invoked when at least one
     * handler listens for {@code eventType}.
     *
     * @return the dispatched event, or {@code null} if nothing listens and no event was created
     */
    public <T extends Event> T call(Class<T> eventType, Supplier<T> supplier) {
        if (eventType == null || supplier == null || !hasListeners(eventType)) {
            return null;
        }
        return call(supplier.get());
    }

    /**
     * Lazily constructs and dispatches an event. Alias for {@link #call(Class, Supplier)}.
     */
    public <T extends Event> T post(Class<T> eventType, Supplier<T> supplier) {
        return call(eventType, supplier);
    }

    /**
     * Lazily constructs and dispatches an event. Alias for {@link #call(Class, Supplier)}.
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

    private void scanType(Object listener, Class<?> type, Class<? extends Event> eventClass,
                          Set<Class<?>> visitedTypes, Set<MethodSignature> seenSignatures, boolean staticOnly) {
        if (type == null || type == Object.class || !visitedTypes.add(type)) {
            return;
        }

        for (Method method : type.getDeclaredMethods()) {
            if (method.isSynthetic() || method.isBridge() || !method.isAnnotationPresent(EventTarget.class)) {
                continue;
            }
            if (staticOnly && !Modifier.isStatic(method.getModifiers())) {
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

            if (eventClass != null && !parameterType.equals(eventClass)) {
                continue;
            }

            MethodSignature signature = new MethodSignature(method);
            if (!seenSignatures.add(signature)) {
                continue;
            }

            EventTarget eventTarget = method.getAnnotation(EventTarget.class);
            EventPriority priorityAnnotation = method.getAnnotation(EventPriority.class);
            int priority = priorityAnnotation != null ? priorityAnnotation.value() : DEFAULT_PRIORITY;

            Handler handler = new Handler(
                    listener,
                    method,
                    null,
                    Modifier.isStatic(method.getModifiers()) ? method.getDeclaringClass() : listener,
                    parameterType.asSubclass(Event.class),
                    priority,
                    eventTarget.ignoreCancelled(),
                    registrationOrder.getAndIncrement(),
                    invokerFor(method, listener)
            );
            addHandler(handler);
        }

        for (Field field : type.getDeclaredFields()) {
            if (field.isSynthetic() || !field.isAnnotationPresent(EventTarget.class)) {
                continue;
            }
            if (staticOnly && !Modifier.isStatic(field.getModifiers())) {
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
            if (eventClass != null && !fieldEventType.equals(eventClass)) {
                continue;
            }

            EventTarget eventTarget = field.getAnnotation(EventTarget.class);
            EventPriority priorityAnnotation = field.getAnnotation(EventPriority.class);
            int priority = priorityAnnotation != null ? priorityAnnotation.value() : DEFAULT_PRIORITY;
            EventListener<?> fieldListener = listenerFromField(listener, field);
            if (fieldListener == null) {
                continue;
            }

            final Class<? extends Event> dispatchType = fieldEventType;
            final EventListener<?> dispatchListener = fieldListener;
            Handler handler = new Handler(
                    listener,
                    null,
                    field,
                    Modifier.isStatic(field.getModifiers()) ? field.getDeclaringClass() : listener,
                    dispatchType,
                    priority,
                    eventTarget.ignoreCancelled(),
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

        for (Class<?> iface : type.getInterfaces()) {
            scanType(listener, iface, eventClass, visitedTypes, seenSignatures, staticOnly);
        }
        scanType(listener, type.getSuperclass(), eventClass, visitedTypes, seenSignatures, staticOnly);
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
                method.invoke(isStatic ? null : listener, event);
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
