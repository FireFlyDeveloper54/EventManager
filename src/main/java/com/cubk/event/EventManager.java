package com.cubk.event;

import com.cubk.event.annotations.EventPriority;
import com.cubk.event.annotations.EventTarget;
import com.cubk.event.impl.Cancellable;
import com.cubk.event.impl.Event;
import com.cubk.event.impl.Stoppable;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;

public class EventManager {
    private static final Logger LOGGER = Logger.getLogger(EventManager.class.getName());
    private static final int DEFAULT_PRIORITY = 10;
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
    private final AtomicLong registrationOrder;

    public EventManager() {
        this.eventHandlers = new ConcurrentHashMap<Class<? extends Event>, CopyOnWriteArrayList<Handler>>();
        this.registrationOrder = new AtomicLong();
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

    public void register(Object listener, Class<? extends Event> eventClass) {
        if (listener == null) {
            return;
        }

        Set<Class<?>> visitedTypes = new HashSet<Class<?>>();
        Set<MethodSignature> seenSignatures = new HashSet<MethodSignature>();
        scanType(listener, listener.getClass(), eventClass, visitedTypes, seenSignatures);
    }

    public void unregister(Object listener) {
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

    public void unregister(Class<?> listenerClass) {
        if (listenerClass == null) {
            return;
        }
        removeHandlers(new Predicate<Handler>() {
            @Override
            public boolean test(Handler handler) {
                return listenerClass.isAssignableFrom(handler.listener.getClass());
            }
        });
    }

    public void clear() {
        eventHandlers.clear();
    }

    public <T extends Event> T call(T event) {
        if (event == null) {
            return null;
        }

        LinkedHashSet<Handler> collected = new LinkedHashSet<Handler>();
        collectHandlers(event.getClass(), collected, new HashSet<Class<?>>());

        if (collected.isEmpty()) {
            return event;
        }

        List<Handler> handlers = new ArrayList<Handler>(collected);
        handlers.sort(HANDLER_ORDER);

        Cancellable cancellable = event instanceof Cancellable ? (Cancellable) event : null;
        Stoppable stoppable = event instanceof Stoppable ? (Stoppable) event : null;

        for (Handler handler : handlers) {
            if (stoppable != null && stoppable.isStopped()) {
                break;
            }

            if (cancellable != null && cancellable.isCancelled() && handler.ignoreCancelled) {
                continue;
            }

            try {
                handler.invoke(event);
            } catch (Throwable t) {
                LOGGER.log(Level.SEVERE, "Failed to invoke event handler " + handler.method, t);
            }
        }

        return event;
    }

    private void scanType(Object listener, Class<?> type, Class<? extends Event> eventClass,
                          Set<Class<?>> visitedTypes, Set<MethodSignature> seenSignatures) {
        if (type == null || type == Object.class || !visitedTypes.add(type)) {
            return;
        }

        for (Method method : type.getDeclaredMethods()) {
            if (!method.isAnnotationPresent(EventTarget.class) || method.getParameterTypes().length != 1) {
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
                    parameterType.asSubclass(Event.class),
                    priority,
                    eventTarget.ignoreCancelled(),
                    registrationOrder.getAndIncrement(),
                    createInvoker(method)
            );
            addHandler(handler);
        }

        for (Class<?> iface : type.getInterfaces()) {
            scanType(listener, iface, eventClass, visitedTypes, seenSignatures);
        }
        scanType(listener, type.getSuperclass(), eventClass, visitedTypes, seenSignatures);
    }

    private void addHandler(Handler handler) {
        CopyOnWriteArrayList<Handler> handlers = eventHandlers.computeIfAbsent(handler.eventType, k -> new CopyOnWriteArrayList<Handler>());
        synchronized (handlers) {
            if (handlers.contains(handler)) {
                return;
            }

            int index = 0;
            while (index < handlers.size() && HANDLER_ORDER.compare(handlers.get(index), handler) <= 0) {
                index++;
            }
            handlers.add(index, handler);
        }
    }

    private void removeHandlers(Predicate<Handler> predicate) {
        for (CopyOnWriteArrayList<Handler> handlers : eventHandlers.values()) {
            handlers.removeIf(predicate);
        }
        eventHandlers.entrySet().removeIf(entry -> entry.getValue().isEmpty());
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

    private static Invoker createInvoker(Method method) {
        try {
            method.setAccessible(true);
        } catch (SecurityException ignored) {
            // Fall through to reflection.
        }

        try {
            final MethodHandle handle = MethodHandles.lookup().unreflect(method);
            return new Invoker() {
                @Override
                public void invoke(Object listener, Event event) throws Throwable {
                    handle.invoke(listener, event);
                }
            };
        } catch (IllegalAccessException ex) {
            return new Invoker() {
                @Override
                public void invoke(Object listener, Event event) throws Throwable {
                    method.invoke(listener, event);
                }
            };
        }
    }

    private interface Invoker {
        void invoke(Object listener, Event event) throws Throwable;
    }

    private static final class Handler {
        private final Object listener;
        private final Method method;
        private final Class<? extends Event> eventType;
        private final int priority;
        private final boolean ignoreCancelled;
        private final long order;
        private final Invoker invoker;

        private Handler(Object listener, Method method, Class<? extends Event> eventType, int priority,
                        boolean ignoreCancelled, long order, Invoker invoker) {
            this.listener = listener;
            this.method = method;
            this.eventType = eventType;
            this.priority = priority;
            this.ignoreCancelled = ignoreCancelled;
            this.order = order;
            this.invoker = invoker;
        }

        private void invoke(Event event) throws Throwable {
            invoker.invoke(listener, event);
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
            return listener == other.listener && method.equals(other.method);
        }

        @Override
        public int hashCode() {
            return 31 * System.identityHashCode(listener) + method.hashCode();
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
