package dev.hotaru.event;

import dev.hotaru.event.impl.Event;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Bridges {@link EventManager} to JDK 9+ {@code java.util.concurrent.Flow.Publisher}
 * using dynamic proxies and reflection, enabling full Reactive Streams compatibility
 * while compiling cleanly on JDK 8.
 */
public final class FlowPublisherAdapter {

    private static final boolean FLOW_SUPPORTED;
    private static final Class<?> FLOW_PUBLISHER_CLASS;
    private static final Class<?> FLOW_SUBSCRIBER_CLASS;
    private static final Class<?> FLOW_SUBSCRIPTION_CLASS;
    private static final Method ON_SUBSCRIBE_METHOD;
    private static final Method ON_NEXT_METHOD;
    private static final Method ON_ERROR_METHOD;

    static {
        boolean supported = false;
        Class<?> pub = null;
        Class<?> sub = null;
        Class<?> subs = null;
        Method onSub = null;
        Method onNxt = null;
        Method onErr = null;
        try {
            pub = Class.forName("java.util.concurrent.Flow$Publisher");
            sub = Class.forName("java.util.concurrent.Flow$Subscriber");
            subs = Class.forName("java.util.concurrent.Flow$Subscription");
            onSub = sub.getMethod("onSubscribe", subs);
            onNxt = sub.getMethod("onNext", Object.class);
            onErr = sub.getMethod("onError", Throwable.class);
            supported = true;
        } catch (Throwable ignored) {
            // Flow API not present on JDK 8
        }
        FLOW_SUPPORTED = supported;
        FLOW_PUBLISHER_CLASS = pub;
        FLOW_SUBSCRIBER_CLASS = sub;
        FLOW_SUBSCRIPTION_CLASS = subs;
        ON_SUBSCRIBE_METHOD = onSub;
        ON_NEXT_METHOD = onNxt;
        ON_ERROR_METHOD = onErr;
    }

    private FlowPublisherAdapter() {}

    /**
     * Returns true if {@code java.util.concurrent.Flow} is present on the current runtime (JDK 9+).
     */
    public static boolean isSupported() {
        return FLOW_SUPPORTED;
    }

    /**
     * Adapts the specified event type on the bus to a {@code java.util.concurrent.Flow.Publisher}.
     *
     * @param bus       the event bus
     * @param eventType the event class to stream
     * @param <T>       the event type
     * @return a proxy implementing {@code java.util.concurrent.Flow.Publisher<T>}
     * @throws UnsupportedOperationException if running on JDK 8 where Flow is not available
     */
    public static <T extends Event> Object asFlowPublisher(final EventManager bus, final Class<T> eventType) {
        Objects.requireNonNull(bus, "bus");
        Objects.requireNonNull(eventType, "eventType");
        if (!FLOW_SUPPORTED) {
            throw new UnsupportedOperationException(
                    "java.util.concurrent.Flow is not available on this JVM runtime (requires JDK 9+)");
        }

        InvocationHandler publisherHandler = new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                String methodName = method.getName();
                if ("subscribe".equals(methodName) && args != null && args.length == 1) {
                    final Object subscriber = args[0];
                    Objects.requireNonNull(subscriber, "subscriber");
                    bindSubscriber(bus, eventType, subscriber);
                    return null;
                }
                if ("toString".equals(methodName)) {
                    return "FlowPublisher[" + eventType.getName() + "]";
                }
                if ("hashCode".equals(methodName)) {
                    return System.identityHashCode(proxy);
                }
                if ("equals".equals(methodName)) {
                    return proxy == (args != null && args.length > 0 ? args[0] : null);
                }
                return null;
            }
        };

        return Proxy.newProxyInstance(
                FLOW_PUBLISHER_CLASS.getClassLoader(),
                new Class<?>[]{FLOW_PUBLISHER_CLASS},
                publisherHandler
        );
    }

    private static <T extends Event> void bindSubscriber(
            final EventManager bus,
            final Class<T> eventType,
            final Object subscriber) throws Exception {

        final AtomicLong demand = new AtomicLong();
        final AtomicBoolean cancelled = new AtomicBoolean();
        // Spec conformance: events arriving with no outstanding demand must be
        // buffered, not dropped — otherwise the publisher silently loses events.
        final java.util.Queue<T> pending = new java.util.concurrent.ConcurrentLinkedQueue<T>();

        final Subscription[] busSubRef = new Subscription[1];

        InvocationHandler subscriptionHandler = new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                String name = method.getName();
                if ("request".equals(name) && args != null && args.length == 1) {
                    long n = (Long) args[0];
                    if (n > 0) {
                        demand.addAndGet(n);
                        drain(busSubRef, subscriber, pending, demand, cancelled);
                    }
                    return null;
                }
                if ("cancel".equals(name)) {
                    if (cancelled.compareAndSet(false, true)) {
                        pending.clear();
                        if (busSubRef[0] != null) {
                            busSubRef[0].unsubscribe();
                        }
                    }
                    return null;
                }
                return null;
            }
        };

        Object flowSubscription = Proxy.newProxyInstance(
                FLOW_SUBSCRIPTION_CLASS.getClassLoader(),
                new Class<?>[]{FLOW_SUBSCRIPTION_CLASS},
                subscriptionHandler
        );

        ON_SUBSCRIBE_METHOD.invoke(subscriber, flowSubscription);

        busSubRef[0] = bus.on(eventType).handle(new Consumer<T>() {
            @Override
            public void accept(T event) {
                if (cancelled.get()) {
                    return;
                }
                pending.add(event);
                drain(busSubRef, subscriber, pending, demand, cancelled);
            }
        });
    }

    private static <T extends Event> void drain(
            final Subscription[] busSubRef,
            final Object subscriber,
            final java.util.Queue<T> pending,
            final AtomicLong demand,
            final AtomicBoolean cancelled) {
        T event;
        while (!cancelled.get() && (event = pending.peek()) != null) {
            // CAS cap: never deliver more than the requested total, even when
            // events and request(n) race concurrently.
            long claimed = demand.getAndUpdate(new java.util.function.LongUnaryOperator() {
                @Override
                public long applyAsLong(long d) {
                    return d > 0 ? d - 1 : d;
                }
            });
            if (claimed <= 0) {
                return;
            }
            pending.poll();
            try {
                ON_NEXT_METHOD.invoke(subscriber, event);
            } catch (Throwable t) {
                // A failing subscriber must be signalled via onError, not
                // silently cancelled.
                cancelled.set(true);
                pending.clear();
                if (busSubRef[0] != null) {
                    busSubRef[0].unsubscribe();
                }
                try {
                    ON_ERROR_METHOD.invoke(subscriber, t instanceof InvocationTargetException ? t.getCause() : t);
                } catch (Throwable ignored) {
                    // Nothing left to do: the subscriber is gone either way.
                }
                return;
            }
        }
    }
}
