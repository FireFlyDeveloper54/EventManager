package dev.hotaru.event;

import dev.hotaru.event.annotations.EventTarget;

import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Static introspection machinery behind {@link EventManager}: scans annotated
 * listener classes into immutable {@link ListenerPlan}s, builds
 * {@link LambdaMetafactory}-backed method invokers with reflective fallbacks,
 * caches field accessors, and resolves generic event types through the
 * listener's concrete type arguments. All state is process-wide and keyed by
 * {@link ClassValue}, so instances carry no per-bus data.
 */
final class ListenerIntrospection {

    private static final Logger log = Logger.getLogger(ListenerIntrospection.class.getName());
    private static final int DEFAULT_PRIORITY = Priority.NORMAL;
    private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();
    private static final Method PRIVATE_LOOKUP_IN = resolvePrivateLookupIn();
    private static final Constructor<MethodHandles.Lookup> JAVA8_LOOKUP_CTOR = resolveJava8LookupConstructor();

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
    private static final ClassValue<ConcurrentMap<Method, InvokerFactory>> INVOKER_FACTORIES =
            new ClassValue<ConcurrentMap<Method, InvokerFactory>>() {
                @Override
                protected ConcurrentMap<Method, InvokerFactory> computeValue(Class<?> type) {
                    return new ConcurrentHashMap<Method, InvokerFactory>();
                }
            };
    private static final ClassValue<ConcurrentMap<Field, FieldAccessor>> FIELD_ACCESSORS =
            new ClassValue<ConcurrentMap<Field, FieldAccessor>>() {
                @Override
                protected ConcurrentMap<Field, FieldAccessor> computeValue(Class<?> type) {
                    return new ConcurrentHashMap<Field, FieldAccessor>();
                }
            };
    private static final ClassValue<ListenerPlan> LISTENER_PLANS = new ClassValue<ListenerPlan>() {
        @Override
        protected ListenerPlan computeValue(Class<?> type) {
            return buildListenerPlan(type);
        }
    };
    private static final ClassValue<EventFilter<Event>> FILTER_CACHE =
            new ClassValue<EventFilter<Event>>() {
                @SuppressWarnings("unchecked")
                @Override
                protected EventFilter<Event> computeValue(Class<?> type) {
                    try {
                        Constructor<?> constructor = type.getDeclaredConstructor();
                        constructor.setAccessible(true);
                        return (EventFilter<Event>) constructor.newInstance();
                    } catch (Throwable t) {
                        throw new IllegalArgumentException("Failed to instantiate EventFilter: " + type, t);
                    }
                }
            };

    private ListenerIntrospection() {
    }

    @SuppressWarnings("unchecked")
    private static EventFilter<Event> filterFor(Class<? extends EventFilter> filterClass) {
        if (filterClass == null || filterClass == EventFilter.PassAll.class) {
            return null;
        }
        return FILTER_CACHE.get(filterClass);
    }

    static ListenerPlan planFor(final Class<?> listenerClass) {
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
            Type methodGenericType = null;
            Type[] genericParamTypes = method.getGenericParameterTypes();
            if (genericParamTypes.length > 0 && genericParamTypes[0] instanceof ParameterizedType) {
                ParameterizedType pt = (ParameterizedType) genericParamTypes[0];
                Type[] typeArgs = pt.getActualTypeArguments();
                if (typeArgs.length > 0) {
                    methodGenericType = resolveType(typeArgs[0], typeArgumentsFor(concreteListenerClass, method.getDeclaringClass()));
                }
            }
            definitions.add(HandlerDefinition.forMethod(
                    method,
                    parameterType.asSubclass(Event.class),
                    priority,
                    eventTarget.ignoreCancelled(),
                    filter,
                    methodGenericType,
                    eventTarget.sticky(),
                    eventTarget.id(),
                    eventTarget.after(),
                    eventTarget.before(),
                    eventTarget.afterClasses(),
                    eventTarget.beforeClasses()
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
            Type fieldGenericType = genericTypeFromField(field, concreteListenerClass);
            definitions.add(HandlerDefinition.forField(
                    field,
                    fieldEventType,
                    priority,
                    listenerPriority,
                    eventTarget.ignoreCancelled(),
                    filter,
                    fieldGenericType,
                    eventTarget.sticky(),
                    eventTarget.id(),
                    eventTarget.after(),
                    eventTarget.before(),
                    eventTarget.afterClasses(),
                    eventTarget.beforeClasses()
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

    static boolean isGenericTypeAssignable(Type expected, Type actual) {
        if (expected == null || expected == Object.class) {
            return true;
        }
        if (actual == null) {
            return false;
        }
        if (expected.equals(actual)) {
            return true;
        }
        if (expected instanceof Class<?> && actual instanceof Class<?>) {
            return ((Class<?>) expected).isAssignableFrom((Class<?>) actual);
        }
        if (expected instanceof Class<?> && actual instanceof ParameterizedType) {
            Type rawActual = ((ParameterizedType) actual).getRawType();
            if (rawActual instanceof Class<?>) {
                return ((Class<?>) expected).isAssignableFrom((Class<?>) rawActual);
            }
        }
        if (expected instanceof ParameterizedType && actual instanceof ParameterizedType) {
            ParameterizedType ptExpected = (ParameterizedType) expected;
            ParameterizedType ptActual = (ParameterizedType) actual;
            if (!isGenericTypeAssignable(ptExpected.getRawType(), ptActual.getRawType())) {
                return false;
            }
            Type[] expectedArgs = ptExpected.getActualTypeArguments();
            Type[] actualArgs = ptActual.getActualTypeArguments();
            if (expectedArgs.length != actualArgs.length) {
                return false;
            }
            for (int i = 0; i < expectedArgs.length; i++) {
                if (!isGenericTypeAssignable(expectedArgs[i], actualArgs[i])) {
                    return false;
                }
            }
            return true;
        }
        if (expected instanceof WildcardType) {
            WildcardType wt = (WildcardType) expected;
            Type[] upper = wt.getUpperBounds();
            for (Type u : upper) {
                if (!isGenericTypeAssignable(u, actual)) {
                    return false;
                }
            }
            Type[] lower = wt.getLowerBounds();
            if (lower.length > 0 && !isGenericTypeAssignable(actual, lower[0])) {
                return false;
            }
            return true;
        }
        return false;
    }

    private static Type genericTypeFromField(Field field, Class<?> listenerClass) {
        Type genericType = field.getGenericType();
        if (!(genericType instanceof ParameterizedType)) {
            return null;
        }
        ParameterizedType parameterizedType = (ParameterizedType) genericType;
        Type[] args = parameterizedType.getActualTypeArguments();
        if (args.length == 0) {
            return null;
        }
        Type eventType = resolveType(args[0], typeArgumentsFor(listenerClass, field.getDeclaringClass()));
        if (eventType instanceof ParameterizedType) {
            ParameterizedType eventPt = (ParameterizedType) eventType;
            Type[] eventArgs = eventPt.getActualTypeArguments();
            if (eventArgs.length > 0) {
                return resolveType(eventArgs[0], typeArgumentsFor(listenerClass, field.getDeclaringClass()));
            }
        }
        return null;
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
        Set<Type> seen = Collections.newSetFromMap(new java.util.IdentityHashMap<Type, Boolean>());
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

    static EventListener<?> listenerFromField(Object listener, Field field, boolean reportFailure) {
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

    static ListenerIntrospection.Invoker invokerFor(Method method, Object listener) {
        return invokerFactoryFor(method).create(listener);
    }

    static InvokerFactory invokerFactoryFor(Method method) {
        ConcurrentMap<Method, InvokerFactory> factories = INVOKER_FACTORIES.get(method.getDeclaringClass());
        InvokerFactory factory = factories.get(method);
        if (factory == null) {
            InvokerFactory created = buildInvokerFactory(method);
            InvokerFactory previous = factories.putIfAbsent(method, created);
            factory = previous != null ? previous : created;
        }
        return factory;
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
    private static Constructor<MethodHandles.Lookup> resolveJava8LookupConstructor() {
        try {
            Constructor<MethodHandles.Lookup> ctor =
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

    interface Invoker {
        void invoke(Event event) throws Throwable;
    }

    interface InvokerFactory {
        Invoker create(Object listener);
    }

    private interface FieldAccessor {
        Object get(Object owner) throws Throwable;
    }

    static final class ListenerPlan {
        final HandlerDefinition[] definitions;

        ListenerPlan(HandlerDefinition[] definitions) {
            this.definitions = definitions;
        }
    }

    static final class HandlerDefinition {
        final Method method;
        final Field field;
        final Class<? extends Event> eventType;
        final int priority;
        final boolean listenerPriority;
        final boolean ignoreCancelled;
        final boolean staticMember;
        final Predicate<Event> filter;
        final Type genericType;
        final boolean sticky;
        final String id;
        final String[] after;
        final String[] before;
        final Class<?>[] afterClasses;
        final Class<?>[] beforeClasses;

        private HandlerDefinition(Method method, Field field, Class<? extends Event> eventType,
                                  int priority, boolean listenerPriority, boolean ignoreCancelled,
                                  boolean staticMember, Predicate<Event> filter, Type genericType,
                                  boolean sticky) {
            this(method, field, eventType, priority, listenerPriority, ignoreCancelled,
                 staticMember, filter, genericType, sticky, "", null, null, null, null);
        }

        private HandlerDefinition(Method method, Field field, Class<? extends Event> eventType,
                                  int priority, boolean listenerPriority, boolean ignoreCancelled,
                                  boolean staticMember, Predicate<Event> filter, Type genericType,
                                  boolean sticky,
                                  String id, String[] after, String[] before,
                                  Class<?>[] afterClasses, Class<?>[] beforeClasses) {
            this.method = method;
            this.field = field;
            this.eventType = eventType;
            this.priority = priority;
            this.listenerPriority = listenerPriority;
            this.ignoreCancelled = ignoreCancelled;
            this.staticMember = staticMember;
            this.filter = filter;
            this.genericType = genericType;
            this.sticky = sticky;
            this.id = id != null ? id : "";
            this.after = after;
            this.before = before;
            this.afterClasses = afterClasses;
            this.beforeClasses = beforeClasses;
        }

        private static HandlerDefinition forMethod(Method method, Class<? extends Event> eventType,
                                                   int priority, boolean ignoreCancelled, Predicate<Event> filter,
                                                   Type genericType, boolean sticky) {
            return forMethod(method, eventType, priority, ignoreCancelled, filter, genericType, sticky,
                             "", null, null, null, null);
        }

        private static HandlerDefinition forMethod(Method method, Class<? extends Event> eventType,
                                                   int priority, boolean ignoreCancelled, Predicate<Event> filter,
                                                   Type genericType, boolean sticky,
                                                   String id, String[] after, String[] before,
                                                   Class<?>[] afterClasses, Class<?>[] beforeClasses) {
            return new HandlerDefinition(
                    method,
                    null,
                    eventType,
                    priority,
                    false,
                    ignoreCancelled,
                    Modifier.isStatic(method.getModifiers()),
                    filter,
                    genericType,
                    sticky,
                    id, after, before, afterClasses, beforeClasses
            );
        }

        private static HandlerDefinition forField(Field field, Class<? extends Event> eventType,
                                                  int priority, boolean listenerPriority,
                                                  boolean ignoreCancelled, Predicate<Event> filter,
                                                  Type genericType, boolean sticky) {
            return forField(field, eventType, priority, listenerPriority, ignoreCancelled, filter,
                            genericType, sticky, "", null, null, null, null);
        }

        private static HandlerDefinition forField(Field field, Class<? extends Event> eventType,
                                                  int priority, boolean listenerPriority,
                                                  boolean ignoreCancelled, Predicate<Event> filter,
                                                  Type genericType, boolean sticky,
                                                  String id, String[] after, String[] before,
                                                  Class<?>[] afterClasses, Class<?>[] beforeClasses) {
            return new HandlerDefinition(
                    null,
                    field,
                    eventType,
                    priority,
                    listenerPriority,
                    ignoreCancelled,
                    Modifier.isStatic(field.getModifiers()),
                    filter,
                    genericType,
                    sticky,
                    id, after, before, afterClasses, beforeClasses
            );
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
}
