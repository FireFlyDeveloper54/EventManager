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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.logging.Level;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.java.Log;

@Log
public class EventManager {
    private static final int DEFAULT_PRIORITY = Priority.NORMAL;
    private static final Handler[] NO_HANDLERS = new Handler[0];
    private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();

    private static final Method PRIVATE_LOOKUP_IN = resolvePrivateLookupIn();
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

    private final Map<Class<? extends Event>, Handler[]> eventHandlers = new ConcurrentHashMap<Class<? extends Event>, Handler[]>();
    private final Map<Class<?>, CachedDispatch> dispatchCache = new ConcurrentHashMap<Class<?>, CachedDispatch>();
    private final Map<Method, InvokerFactory> invokerFactories = new ConcurrentHashMap<Method, InvokerFactory>();
    private final Map<Class<?>, ListenerPlan> listenerPlans = new ConcurrentHashMap<Class<?>, ListenerPlan>();
    private final AtomicLong registrationOrder = new AtomicLong();
    private final AtomicLong mutationVersion = new AtomicLong();
    private volatile EventErrorHandler errorHandler = DEFAULT_ERROR_HANDLER;

    public EventManager() {
    }

    public EventManager(EventErrorHandler errorHandler) {
        this.errorHandler = errorHandler != null ? errorHandler : DEFAULT_ERROR_HANDLER;
    }

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
        register(listener, (Class<? extends Event>) null);
    }

    public void register(Object listener, Class<? extends Event> eventClass) {
        if (listener == null) {
            return;
        }
        if (listener instanceof Class<?>) {
            register((Class<?>) listener, eventClass);
            return;
        }
        bindListenerPlan(listener, listenerPlanFor(listener.getClass()), eventClass, false);
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

    public Subscription subscribe(Object listener) {
        if (listener == null) {
            return Subscription.NOOP;
        }
        if (listener instanceof Class<?>) {
            return subscribe((Class<?>) listener);
        }
        register(listener);
        return isRegistered(listener) ? new ListenerSubscription(listener, null) : Subscription.NOOP;
    }

    public Subscription subscribe(Class<?> listenerClass) {
        if (listenerClass == null) {
            return Subscription.NOOP;
        }
        register(listenerClass);
        return isRegistered(listenerClass) ? new ListenerSubscription(listenerClass, null) : Subscription.NOOP;
    }

    public Subscription subscribe(Object listener, Class<? extends Event> eventClass) {
        if (listener == null || eventClass == null) {
            return Subscription.NOOP;
        }
        if (listener instanceof Class<?>) {
            register((Class<?>) listener, eventClass);
        } else {
            register(listener, eventClass);
        }
        return isRegistered(listener, eventClass)
                ? new ListenerSubscription(listener, eventClass)
                : Subscription.NOOP;
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
                return handler.listener == listener;
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
        removeHandlers(new Predicate<Handler>() {
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
        if (listenerClass == null || eventClass == null) {
            return;
        }
        removeHandlers(new Predicate<Handler>() {
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

    public void clear() {
        boolean hadRegistrations = !eventHandlers.isEmpty() || !dispatchCache.isEmpty();
        eventHandlers.clear();
        dispatchCache.clear();
        listenerPlans.clear();
        invokerFactories.clear();
        if (hadRegistrations) {
            mutationVersion.incrementAndGet();
        }
    }

    public void removeEntry(Class<? extends Event> eventType) {
        if (eventType == null) {
            return;
        }
        if (eventHandlers.remove(eventType) != null) {
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
                if (handlers[i].listener == listener) {
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
            if (handlers[i].listener == listener) {
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
        if (event == null) {
            return null;
        }

        Handler[] handlers = exactHandlersFor(event.getClass());
        if (handlers.length != 0) {
            dispatch(event, handlers);
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

    private void dispatch(Event event, Handler[] handlers) {
        Cancellable cancellable = event instanceof Cancellable ? (Cancellable) event : null;
        Stoppable stoppable = event instanceof Stoppable ? (Stoppable) event : null;

        for (int i = 0; i < handlers.length; i++) {
            Handler handler = handlers[i];

            if (stoppable != null) {
                boolean stopped;
                try {
                    stopped = stoppable.isStopped();
                } catch (Throwable t) {
                    reportFailure(event, event, t);
                    return;
                }
                if (stopped) {
                    break;
                }
            }

            boolean handling;
            try {
                handling = handler.isHandlingEvents();
            } catch (Throwable t) {
                reportFailure(event, handler.listener, t);
                continue;
            }
            if (!handling) {
                continue;
            }

            if (cancellable != null && handler.ignoreCancelled) {
                boolean cancelled;
                try {
                    cancelled = cancellable.isCancelled();
                } catch (Throwable t) {
                    reportFailure(event, event, t);
                    return;
                }
                if (cancelled) {
                    continue;
                }
            }

            try {
                handler.invoke(event);
            } catch (Throwable t) {
                reportFailure(event, handler.listener, t);
            }
        }
    }

    private void reportFailure(Event event, Object source, Throwable throwable) {
        try {
            errorHandler.handle(event, source, throwable);
        } catch (Throwable errorHandlerFailure) {
            log.log(Level.SEVERE, "Event error handler threw while handling a listener failure",
                    errorHandlerFailure);
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
        Handler[] handlers = eventHandlers.get(eventClass);
        return handlers == null ? NO_HANDLERS : handlers;
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
                log.warning("Skipping listener field " + field + " because it is not an EventListener");
                continue;
            }

            Class<? extends Event> fieldEventType = eventTypeFromField(field);
            if (fieldEventType == null) {
                log.warning("Skipping listener field " + field
                        + " because its EventListener event type could not be inferred");
                continue;
            }

            EventTarget eventTarget = field.getAnnotation(EventTarget.class);
            boolean listenerPriority = eventTarget.value() == Priority.UNSPECIFIED;
            int priority = annotationPriority(eventTarget, DEFAULT_PRIORITY);
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
        for (HandlerDefinition definition : plan.definitions) {
            if (eventClass != null && definition.eventType != eventClass) {
                continue;
            }
            bindDefinition(listener, definition, staticOnly);
        }
    }

    private void bindDefinition(Object listener, HandlerDefinition definition, boolean staticOnly) {
        if (staticOnly && !definition.staticMember) {
            return;
        }

        if (definition.method != null) {
            Method method = definition.method;
            Handler handler = new Handler(
                    listener,
                    method,
                    null,
                    definition.staticMember ? method.getDeclaringClass() : listener,
                    definition.eventType,
                    normalizePriority(definition.priority),
                    definition.ignoreCancelled,
                    registrationOrder.getAndIncrement(),
                    invokerFor(method, listener)
            );
            addHandler(handler);
            return;
        }

        Field field = definition.field;
        EventListener<?> fieldListener = listenerFromField(listener, field);
        if (fieldListener == null) {
            return;
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
                normalizePriority(priority),
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

    private void addHandler(final Handler handler) {
        final boolean[] added = new boolean[1];
        eventHandlers.compute(handler.eventType, new java.util.function.BiFunction<Class<? extends Event>, Handler[], Handler[]>() {
            @Override
            public Handler[] apply(Class<? extends Event> key, Handler[] existing) {
                Handler[] updated = insertHandler(existing, handler);
                if (updated != existing) {
                    added[0] = true;
                }
                return updated;
            }
        });
        if (added[0]) {
            mutationVersion.incrementAndGet();
        }
    }

    private void removeHandlers(final Predicate<Handler> predicate) {
        final boolean[] removed = new boolean[1];
        for (Class<? extends Event> eventType : eventHandlers.keySet()) {
            eventHandlers.computeIfPresent(eventType, new java.util.function.BiFunction<Class<? extends Event>, Handler[], Handler[]>() {
                @Override
                public Handler[] apply(Class<? extends Event> key, Handler[] handlers) {
                    Handler[] updated = removeMatching(handlers, predicate);
                    if (updated != handlers) {
                        removed[0] = true;
                    }
                    return updated.length == 0 ? null : updated;
                }
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
        Handler[] handlers = eventHandlers.get(eventType);
        if (handlers != null) {
            for (int i = 0; i < handlers.length; i++) {
                collected.add(handlers[i]);
            }
        }

        for (Class<?> iface : type.getInterfaces()) {
            collectHandlers(iface, collected, visitedTypes);
        }
        collectHandlers(type.getSuperclass(), collected, visitedTypes);
    }

    private EventListener<?> listenerFromField(Object listener, Field field) {
        boolean isStatic = Modifier.isStatic(field.getModifiers());
        boolean wasAccessible = field.isAccessible();
        try {
            field.setAccessible(true);
        } catch (SecurityException ignored) {

        }

        try {
            Object value = field.get(isStatic ? null : listener);
            if (value == null) {
                log.warning("Skipping listener field " + field + " because its value is null");
                return null;
            }
            if (!(value instanceof EventListener<?>)) {
                log.warning("Skipping listener field " + field + " because its value is not an EventListener");
                return null;
            }
            return (EventListener<?>) value;
        } catch (IllegalAccessException e) {
            log.log(Level.WARNING, "Skipping listener field " + field + " because it is not accessible", e);
            return null;
        } finally {
            try {
                field.setAccessible(wasAccessible);
            } catch (SecurityException ignored) {

            }
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

    private static boolean sameMethod(Method left, Method right) {
        if (left == null || right == null) {
            return false;
        }
        if (left.equals(right)) {
            return true;
        }
        return left.getName().equals(right.getName())
                && java.util.Arrays.equals(left.getParameterTypes(), right.getParameterTypes());
    }

    private static boolean sameField(Field left, Field right) {
        if (left == null || right == null) {
            return false;
        }
        if (left.equals(right)) {
            return true;
        }
        return left.getName().equals(right.getName()) && left.getType() == right.getType();
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

    private Invoker invokerFor(Method method, Object listener) {
        InvokerFactory factory = invokerFactories.computeIfAbsent(method, new java.util.function.Function<Method, InvokerFactory>() {
            @Override
            public InvokerFactory apply(Method key) {
                return buildInvokerFactory(key);
            }
        });
        return factory.create(listener);
    }

    private static InvokerFactory buildInvokerFactory(final Method method) {
        final boolean isStatic = Modifier.isStatic(method.getModifiers());

        try {
            method.setAccessible(true);
        } catch (SecurityException ignored) {

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
                    @SneakyThrows
                    public void invoke(Event event) {
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
                        @SneakyThrows
                        public void invoke(Event event) {
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
            @SneakyThrows
            public void invoke(Event event) {
                try {
                    method.invoke(isStatic ? null : listener, event);
                } catch (InvocationTargetException invocationFailure) {
                    Throwable cause = invocationFailure.getCause();
                    throw cause != null ? cause : invocationFailure;
                }
            }
        };
    }

    @SneakyThrows
    private static Method resolvePrivateLookupIn() {
        try {
            return MethodHandles.class.getMethod("privateLookupIn", Class.class, MethodHandles.Lookup.class);
        } catch (NoSuchMethodException javaEight) {
            return null;
        }
    }

    @SneakyThrows
    private static MethodHandles.Lookup lookupFor(Class<?> declaringClass) {
        if (PRIVATE_LOOKUP_IN != null) {
            try {
                return (MethodHandles.Lookup) PRIVATE_LOOKUP_IN.invoke(null, declaringClass, LOOKUP);
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

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private final class HandlerSubscription implements Subscription {
        private final Handler handler;
        private final AtomicBoolean subscribed = new AtomicBoolean(true);

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

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private final class ListenerSubscription implements Subscription {
        private final Object listener;
        private final Class<? extends Event> eventClass;
        private final AtomicBoolean subscribed = new AtomicBoolean(true);

        @Override
        public void unsubscribe() {
            if (!subscribed.compareAndSet(true, false)) {
                return;
            }
            if (eventClass == null) {
                EventManager.this.unregister(listener);
            } else {
                EventManager.this.unregister(listener, eventClass);
            }
        }

        @Override
        public boolean isSubscribed() {
            if (!subscribed.get()) {
                return false;
            }
            return eventClass == null
                    ? EventManager.this.isRegistered(listener)
                    : EventManager.this.isRegistered(listener, eventClass);
        }
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    @FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
    private static final class ListenerPlan {
        HandlerDefinition[] definitions;
    }

    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    @FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
    private static final class HandlerDefinition {
        Method method;
        Field field;
        Class<? extends Event> eventType;
        int priority;
        boolean listenerPriority;
        boolean ignoreCancelled;
        boolean staticMember;

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

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    @FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
    private static final class CachedDispatch {
        long version;
        Handler[] handlers;
    }

    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    @FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
    private static final class Handler {
        Object listener;
        Method method;
        Field field;
        Object dedupeOwner;
        Class<? extends Event> eventType;
        int priority;
        boolean ignoreCancelled;
        long order;
        Invoker invoker;

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

    @EqualsAndHashCode
    @FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
    private static final class MethodSignature {
        String name;
        Class<?>[] parameterTypes;

        private MethodSignature(Method method) {
            this.name = method.getName();
            this.parameterTypes = method.getParameterTypes();
        }
    }
}
