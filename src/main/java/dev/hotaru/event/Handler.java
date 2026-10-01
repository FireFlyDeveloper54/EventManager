package dev.hotaru.event;

import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * A single registered event handler: the bound listener (or weak reference),
 * its dispatch metadata (event type, priority, ordering constraints), and its
 * invocation strategy.
 *
 * <p>Package-private: part of the event bus internals. The public registration
 * API lives on {@link EventManager}; storage and lookup live in
 * {@link HandlerRegistry}.
 */
final class Handler {

    /** Shared empty handler array, returned instead of allocating. */
    static final Handler[] NO_HANDLERS = new Handler[0];
    final Object listener;
    final WeakReference<Object> weakListener;
    final Method method;
    final Field field;
    final Object dedupeOwner;
    final Class<? extends Event> eventType;
    final int priority;
    final boolean ignoreCancelled;
    final long order;
    final boolean once;
    final Predicate<Event> filter;
    final ListenerIntrospection.Invoker invoker;
    final EventSubscriber subscriber;
    final Type genericType;
    final String id;
    final String[] after;
    final String[] before;
    final Class<?>[] afterClasses;
    final Class<?>[] beforeClasses;
    final AtomicBoolean active = new AtomicBoolean(true);

    Handler(Object listener, boolean weak, Method method, Field field, Object dedupeOwner,
                    Class<? extends Event> eventType, int priority, boolean ignoreCancelled,
                    long order, boolean once, Predicate<Event> filter, Type genericType, ListenerIntrospection.Invoker invoker) {
        this(listener, weak, method, field, dedupeOwner, eventType, priority, ignoreCancelled,
             order, once, filter, genericType, "", null, null, null, null, invoker);
    }

    Handler(Object listener, boolean weak, Method method, Field field, Object dedupeOwner,
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

    Handler(Object listener, boolean weak, Method method, Field field, Object dedupeOwner,
                    Class<? extends Event> eventType, int priority, boolean ignoreCancelled,
                    long order, boolean once, Predicate<Event> filter, ListenerIntrospection.Invoker invoker) {
        this(listener, weak, method, field, dedupeOwner, eventType, priority, ignoreCancelled, order, once, filter, null, invoker);
    }

    Handler(Object listener, Method method, Field field, Object dedupeOwner,
                    Class<? extends Event> eventType, int priority, boolean ignoreCancelled,
                    long order, boolean once, Predicate<Event> filter, ListenerIntrospection.Invoker invoker) {
        this(listener, false, method, field, dedupeOwner, eventType, priority, ignoreCancelled, order, once, filter, invoker);
    }

    boolean isWeak() {
        return weakListener != null;
    }

    boolean isDead() {
        return weakListener != null && weakListener.get() == null;
    }

    Object getListener() {
        return weakListener != null ? weakListener.get() : listener;
    }

    boolean matchesGenericEvent(Event event) {
        if (genericType == null || !(event instanceof GenericEvent<?>)) {
            return true;
        }
        Type actualType = ((GenericEvent<?>) event).getGenericType();
        return ListenerIntrospection.isGenericTypeAssignable(genericType, actualType);
    }

    boolean matchesListener(Object candidate) {
        if (candidate == null) return false;
        if (listener == candidate) return true;
        if (weakListener != null) {
            Object target = weakListener.get();
            return target == candidate;
        }
        return false;
    }

    void invoke(Event event) throws Throwable {
        invoker.invoke(event);
    }

    boolean isHandlingEvents(Event event) {
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
