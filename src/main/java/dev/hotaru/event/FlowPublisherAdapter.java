package dev.hotaru.event;

import dev.hotaru.event.impl.Event;

import java.lang.reflect.InvocationHandler;
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

    static {
        boolean supported = false;
        Class<?> pub = null;
        Class<?> sub = null;
        Class<?> subs = null;
        Method onSub = null;
        Method onNxt = null;
        try {
            pub = Class.forName("java.util.concurrent.Flow$Publisher");
            sub = Class.forName("java.util.concurrent.Flow$Subscriber");
            subs = Class.forName("java.util.concurrent.Flow$Subscription");
            onSub = sub.getMethod("onSubscribe", subs);
            onNxt = sub.getMethod("onNext", Object.class);
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

        final Subscription[] busSubRef = new Subscription[1];

        InvocationHandler subscriptionHandler = new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                String name = method.getName();
                if ("request".equals(name) && args != null && args.length == 1) {
                    long n = (Long) args[0];
                    if (n <= 0) {
                        return null;
                    }
                    demand.addAndGet(n);
                    return null;
                }
                if ("cancel".equals(name)) {
                    if (cancelled.compareAndSet(false, true)) {
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
                long current = demand.get();
                if (current > 0) {
                    demand.decrementAndGet();
                    try {
                        ON_NEXT_METHOD.invoke(subscriber, event);
                    } catch (Throwable t) {
                        cancelled.set(true);
                        if (busSubRef[0] != null) {
                            busSubRef[0].unsubscribe();
                        }
                    }
                }
            }
        });
    }
}
