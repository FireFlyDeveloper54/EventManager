package dev.hotaru.event;

import org.junit.jupiter.api.Test;

import java.lang.ref.WeakReference;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression tests for the second review round (bus state lifecycle,
 * async-channel error visibility, weak-handler auto purge, weak-keyed
 * metrics, CircuitBreaker).
 */
public class EventManagerFixesRound2Test {

    // Fix 1: clear() must reset handlers, sticky events, upcasters,
    // interceptors and metrics — not just the handler table.
    @Test
    void clearResetsAllBusState() {
        EventManager bus = new EventManager();
        final AtomicInteger invocations = new AtomicInteger();
        bus.register(EventManagerTest.Ping.class, new Consumer<EventManagerTest.Ping>() {
            @Override
            public void accept(EventManagerTest.Ping ping) {
                invocations.incrementAndGet();
            }
        });
        bus.dispatch(new EventManagerTest.Ping());
        assertEquals(1, invocations.get());

        // Populate every other kind of state.
        bus.dispatchSticky(new EventManagerTest.Ping());
        bus.registerUpcaster(EventManagerTest.Ping.class, EventManagerTest.AdminPing.class,
                new java.util.function.Function<EventManagerTest.Ping, EventManagerTest.AdminPing>() {
                    @Override
                    public EventManagerTest.AdminPing apply(EventManagerTest.Ping ping) {
                        return new EventManagerTest.AdminPing();
                    }
                });
        final AtomicInteger intercepted = new AtomicInteger();
        bus.addInterceptor(new EventInterceptor() {
            @Override
            public void intercept(Event event, Runnable proceed) {
                intercepted.incrementAndGet();
                proceed.run();
            }
        });
        final int invBeforeClear = invocations.get();
        bus.dispatch(new EventManagerTest.Ping());

        // Sanity: every kind of state was populated.
        assertTrue(invocations.get() > invBeforeClear);
        assertNotNull(bus.getSticky(EventManagerTest.Ping.class));
        assertTrue(bus.metrics().getDispatchedEvents() > 0);
        assertNotNull(bus.getInterceptor());
        assertTrue(intercepted.get() > 0);

        bus.clear();

        assertEquals(0, bus.handlerCount(), "handlers must be cleared");
        assertNull(bus.getSticky(EventManagerTest.Ping.class), "sticky events must be cleared");
        assertEquals(0, bus.metrics().getDispatchedEvents(), "metrics must be reset");
        assertNull(bus.getInterceptor(), "interceptors must be cleared");

        // Cleared handlers must not fire anymore.
        final int invAfterClear = invocations.get();
        bus.dispatch(new EventManagerTest.Ping());
        assertEquals(invAfterClear, invocations.get(), "cleared handlers must not fire");

        // The upcaster must be gone: an AdminPing listener registered after
        // clear() must not fire for a Ping dispatch.
        final AtomicInteger upcasted = new AtomicInteger();
        bus.register(EventManagerTest.AdminPing.class, new Consumer<EventManagerTest.AdminPing>() {
            @Override
            public void accept(EventManagerTest.AdminPing ping) {
                upcasted.incrementAndGet();
            }
        });
        bus.dispatch(new EventManagerTest.Ping());
        assertEquals(0, upcasted.get(), "upcasters must be cleared");
        bus.close();
    }

    // Fix 1 (continued): close() delegates to clear() and must leave no residue.
    @Test
    void closeResetsAllBusState() {
        EventManager bus = new EventManager();
        bus.register(EventManagerTest.Ping.class, new Consumer<EventManagerTest.Ping>() {
            @Override
            public void accept(EventManagerTest.Ping ping) {
            }
        });
        bus.dispatchSticky(new EventManagerTest.Ping());
        bus.addInterceptor(new EventInterceptor() {
            @Override
            public void intercept(Event event, Runnable proceed) {
                proceed.run();
            }
        });
        bus.dispatch(new EventManagerTest.Ping());

        bus.close();

        assertTrue(bus.isClosed());
        assertEquals(0, bus.handlerCount(), "handlers must be cleared on close");
        assertNull(bus.getSticky(EventManagerTest.Ping.class), "sticky events must be cleared on close");
        assertEquals(0, bus.metrics().getDispatchedEvents(), "metrics must be reset on close");
        assertNull(bus.getInterceptor(), "interceptors must be cleared on close");
    }

    // Fix 2: a Throwable escaping dispatch on the channel worker (here: a
    // throwing interceptor, which dispatch never reports itself) must reach
    // the bus error handler instead of being swallowed, and the worker must
    // stay alive.
    @Test
    void asyncChannelWorkerFailureReachesErrorHandler() throws Exception {
        final AtomicInteger errorCount = new AtomicInteger();
        final CountDownLatch errorSeen = new CountDownLatch(1);
        EventErrorHandler errorHandler = new EventErrorHandler() {
            @Override
            public void handle(Event event, Object listener, Throwable throwable) {
                errorCount.incrementAndGet();
                errorSeen.countDown();
            }
        };
        final EventManager bus = new EventManager(errorHandler);
        bus.setInterceptor(new EventInterceptor() {
            @Override
            public void intercept(Event event, Runnable proceed) {
                throw new IllegalStateException("interceptor boom");
            }
        });
        AsyncEventChannel<EventManagerTest.Ping> channel = new AsyncEventChannel<EventManagerTest.Ping>(
                bus, EventManagerTest.Ping.class, 16, BackpressurePolicy.BLOCK);
        try {
            channel.publish(new EventManagerTest.Ping());
            assertTrue(errorSeen.await(5, TimeUnit.SECONDS),
                    "worker failure must reach the bus error handler");
            assertEquals(1, errorCount.get(), "worker-level failure must be reported exactly once");

            // The worker must have survived: drop the bad interceptor and
            // expect a subsequent event to be delivered.
            bus.setInterceptor(null);
            final AtomicInteger received = new AtomicInteger();
            bus.register(EventManagerTest.Ping.class, new Consumer<EventManagerTest.Ping>() {
                @Override
                public void accept(EventManagerTest.Ping ping) {
                    received.incrementAndGet();
                }
            });
            channel.publish(new EventManagerTest.Ping());
            long deadline = System.currentTimeMillis() + 5000;
            while (received.get() == 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            assertEquals(1, received.get(), "worker thread must survive a dispatch failure");
        } finally {
            channel.close();
        }
    }

    // Fix 3: dead weak handlers are reaped automatically by the amortized
    // purge (at most one scan per 1024 dispatches) without manual intervention.
    @Test
    void weakHandlersArePurgedAutomatically() throws Exception {
        EventManager bus = new EventManager();
        Consumer<EventManagerTest.Ping> action = new Consumer<EventManagerTest.Ping>() {
            @Override
            public void accept(EventManagerTest.Ping ping) {
            }
        };
        WeakReference<Consumer<EventManagerTest.Ping>> ref =
                new WeakReference<Consumer<EventManagerTest.Ping>>(action);
        bus.registerWeak(EventManagerTest.Ping.class, action);
        action = null;

        for (int i = 0; i < 100 && ref.get() != null; i++) {
            System.gc();
            Thread.sleep(20);
        }
        assertNull(ref.get(), "test setup: the weak listener should have been collected");

        for (int i = 0; i < 1050; i++) {
            bus.dispatch(new EventManagerTest.Ping());
        }
        assertEquals(0, bus.purgeDeadHandlers(),
                "dead weak handler should already have been auto-purged");
        bus.close();
    }

    // Fix 4: per-type metrics (and mutation stamps) must not pin event classes:
    // after unregistering, a class loaded by a disposable class loader must
    // become unloadable again.
    @Test
    void metricsDoNotPinEventClasses() throws Exception {
        final String pingName = "dev.hotaru.event.EventManagerTest$Ping";
        URL classesDir = EventManagerTest.Ping.class.getProtectionDomain()
                .getCodeSource().getLocation();
        final ClassLoader parent = EventManagerFixesRound2Test.class.getClassLoader();
        URLClassLoader child = new URLClassLoader(new URL[] { classesDir }, parent) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                synchronized (getClassLoadingLock(name)) {
                    Class<?> c = findLoadedClass(name);
                    if (c == null) {
                        if (name.equals(pingName)) {
                            c = findClass(name);
                        } else {
                            c = super.loadClass(name, false);
                        }
                    }
                    if (resolve) {
                        resolveClass(c);
                    }
                    return c;
                }
            }
        };

        EventManager bus = new EventManager();
        @SuppressWarnings("unchecked")
        Class<? extends Event> pingType = (Class<? extends Event>) Class.forName(pingName, true, child);
        Constructor<?> ctor = pingType.getDeclaredConstructor();
        ctor.setAccessible(true);
        Event instance = (Event) ctor.newInstance();
        // Register a handler so removeEntry() also evicts the dispatch-cache
        // entry (it only invalidates the cache when handlers were removed).
        @SuppressWarnings({ "unchecked", "rawtypes" })
        Subscription sub = bus.register(pingType, new Consumer() {
            @Override
            public void accept(Object o) {
            }
        });
        bus.dispatch(instance);
        assertTrue(bus.metrics(pingType).getDispatchedEvents() >= 1,
                "metrics should have been recorded for the event type");

        bus.removeEntry(pingType);

        WeakReference<Class<?>> classRef = new WeakReference<Class<?>>(pingType);
        // Drop every strong reference to the child-loaded class.
        instance = null;
        ctor = null;
        pingType = null;
        sub = null;
        child = null;
        for (int i = 0; i < 100 && classRef.get() != null; i++) {
            System.gc();
            Thread.sleep(50);
        }
        assertNull(classRef.get(),
                "event class must be unloadable after unregister when only weak keys reference it");
        bus.close();
    }

    // Fix 5: removing the misleading padding must not change behavior.
    @Test
    void circuitBreakerTransitions() throws Exception {
        CircuitBreaker cb = new CircuitBreaker(2, 60, TimeUnit.MILLISECONDS);
        assertEquals(CircuitBreaker.State.CLOSED, cb.getState());
        assertTrue(cb.allowExecution());
        cb.recordFailure();
        assertTrue(cb.allowExecution());
        assertEquals(1, cb.getFailureCount());
        cb.recordFailure();
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());
        assertFalse(cb.allowExecution());

        Thread.sleep(120);
        assertTrue(cb.allowExecution(), "cooldown expiry should admit a half-open probe");
        assertEquals(CircuitBreaker.State.HALF_OPEN, cb.getState());
        assertFalse(cb.allowExecution(), "only one half-open probe at a time");
        cb.recordSuccess();
        assertEquals(CircuitBreaker.State.CLOSED, cb.getState());
        assertEquals(0, cb.getFailureCount());

        cb.reset();
        assertEquals(CircuitBreaker.State.CLOSED, cb.getState());
    }
}
