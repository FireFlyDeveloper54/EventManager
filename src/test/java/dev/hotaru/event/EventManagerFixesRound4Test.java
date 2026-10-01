package dev.hotaru.event;

import dev.hotaru.event.impl.Event;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round 4: removeEntry cache invalidation without handlers, and DispatchFrame
 * hardening (at-most-once proceed, race-free async proceed).
 */
public class EventManagerFixesRound4Test {

    static final class ProbeEvent implements Event {
    }

    @SuppressWarnings("unchecked")
    private static ConcurrentMap<Class<?>, ?> dispatchCacheOf(EventManager bus) throws Exception {
        java.lang.reflect.Field f = EventManager.class.getDeclaredField("dispatchCache");
        f.setAccessible(true);
        return (ConcurrentMap<Class<?>, ?>) f.get(bus);
    }

    @Test
    void removeEntryDropsWarmedCacheEntryWithoutHandlers() throws Exception {
        EventManager bus = new EventManager();
        // Warm the dispatch cache for a type that never had a handler.
        bus.dispatch(new ProbeEvent());
        assertTrue(dispatchCacheOf(bus).containsKey(ProbeEvent.class),
                "dispatch should have warmed the cache entry");

        bus.removeEntry(ProbeEvent.class);

        assertFalse(dispatchCacheOf(bus).containsKey(ProbeEvent.class),
                "removeEntry must drop the warmed cache entry even when no handler was removed");
    }

    @Test
    void removeEntryWithHandlersStillDeactivatesAndInvalidates() throws Exception {
        EventManager bus = new EventManager();
        AtomicInteger count = new AtomicInteger();
        bus.register(ProbeEvent.class, e -> count.incrementAndGet());
        bus.dispatch(new ProbeEvent());
        assertEquals(1, count.get());
        assertTrue(dispatchCacheOf(bus).containsKey(ProbeEvent.class));

        bus.removeEntry(ProbeEvent.class);

        assertFalse(dispatchCacheOf(bus).containsKey(ProbeEvent.class));
        bus.dispatch(new ProbeEvent());
        assertEquals(1, count.get(), "removed handler must stay deactivated");
    }

    @Test
    void doubleProceedThrowsIllegalStateException() {
        EventManager bus = new EventManager();
        AtomicInteger count = new AtomicInteger();
        bus.register(ProbeEvent.class, e -> count.incrementAndGet());
        bus.addInterceptor((event, proceed) -> {
            proceed.run();
            assertThrows(IllegalStateException.class, proceed::run,
                    "second proceed() must fail fast");
        });

        bus.dispatch(new ProbeEvent());

        assertEquals(1, count.get(), "handler must run exactly once");
    }

    @Test
    void asyncProceedOnceDeliversEvent() throws Exception {
        EventManager bus = new EventManager();
        AtomicInteger count = new AtomicInteger();
        bus.register(ProbeEvent.class, e -> count.incrementAndGet());
        AtomicReference<Runnable> captured = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        bus.addInterceptor((event, proceed) -> captured.set(proceed));

        bus.dispatch(new ProbeEvent());
        assertEquals(0, count.get(), "no dispatch until proceed is invoked");

        Thread t = new Thread(() -> {
            captured.get().run();
            done.countDown();
        });
        t.start();
        assertTrue(done.await(5, TimeUnit.SECONDS), "async proceed did not complete");
        t.join(5000);

        assertEquals(1, count.get(), "async proceed must deliver the event exactly once");
    }

    @Test
    void shortCircuitWithoutProceedSkipsDispatch() {
        EventManager bus = new EventManager();
        AtomicInteger count = new AtomicInteger();
        bus.register(ProbeEvent.class, e -> count.incrementAndGet());
        bus.addInterceptor((event, proceed) -> {
            // short-circuit: never proceed
        });

        bus.dispatch(new ProbeEvent());

        assertEquals(0, count.get());
        // Pool must stay usable afterwards.
        bus.dispatch(new ProbeEvent());
        assertEquals(0, count.get());
    }
}
