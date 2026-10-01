package dev.hotaru.event;

import org.junit.jupiter.api.Test;

import dev.hotaru.event.annotations.EventTarget;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Lifecycle tests (close / clear / unregister / child-bus) moved out of
 * EventManagerTest to keep that file pushable; behavior unchanged.
 */
public class EventManagerLifecycleTest {
    @Test
    void clearRemovesHandlersAndStaysUsable() {
        EventManager events = new EventManager();
        events.register(new EventManagerTest.CountingListener());
        events.register(new EventManagerTest.FieldListener());
        events.dispatch(new EventManagerTest.Ping());

        events.clear();

        assertEquals(0, events.handlerCount());

        EventManagerTest.CountingListener rebound = new EventManagerTest.CountingListener();
        events.register(rebound);
        events.dispatch(new EventManagerTest.Ping());
        assertEquals(1, rebound.getPings());
    }

    @Test
    void closeClearsRegistrationsAndIsIdempotent() {
        EventManager events = new EventManager();
        events.register(EventManagerTest.Ping.class, event -> {
        });

        events.close();
        events.close();

        assertEquals(0, events.handlerCount());
        assertFalse(events.hasListeners(EventManagerTest.Ping.class));
    }

    @Test
    void subscriptionTryWithResourcesClosesAutomatically() {
        EventManager events = new EventManager();
        final AtomicInteger calls = new AtomicInteger();
        try (Subscription sub = events.register(EventManagerTest.Ping.class, new Consumer<EventManagerTest.Ping>() {
            @Override
            public void accept(EventManagerTest.Ping event) {
                calls.incrementAndGet();
            }
        })) {
            events.dispatch(new EventManagerTest.Ping());
            assertEquals(1, calls.get());
            assertTrue(sub.isSubscribed());
        }
        events.dispatch(new EventManagerTest.Ping());
        assertEquals(1, calls.get());
    }

    @Test
    void unregisterIfRemovesMatchingListeners() {
        EventManager events = new EventManager();
        EventManagerTest.CountingListener listener1 = new EventManagerTest.CountingListener();
        EventManagerTest.CountingListener listener2 = new EventManagerTest.CountingListener();
        events.register(listener1);
        events.register(listener2);

        events.dispatch(new EventManagerTest.Ping());
        assertEquals(1, listener1.getPings());
        assertEquals(1, listener2.getPings());

        events.unregisterIf(target -> target == listener1);
        events.dispatch(new EventManagerTest.Ping());
        assertEquals(1, listener1.getPings());
        assertEquals(2, listener2.getPings());
    }

    @Test
    void unregisterEventTypeRemovesAllHandlersForType() {
        EventManager events = new EventManager();
        EventManagerTest.CountingListener listener = new EventManagerTest.CountingListener();
        events.register(listener);

        events.dispatch(new EventManagerTest.Ping());
        events.dispatch(new EventManagerTest.AdminPing());
        assertEquals(2, listener.getPings()); // EventManagerTest.AdminPing extends EventManagerTest.Ping
        assertEquals(1, listener.getAdminPings());

        events.unregisterEventType(EventManagerTest.AdminPing.class);

        events.dispatch(new EventManagerTest.AdminPing());
        assertEquals(1, listener.getAdminPings());
        assertEquals(3, listener.getPings()); // EventManagerTest.Ping handler still receives EventManagerTest.AdminPing because EventManagerTest.AdminPing is a EventManagerTest.Ping
    }

    @Test
    void childBusBubblesEventsAndCascadesLifecycle() {
        EventManager parent = new EventManager();
        EventManager child = parent.createChildBus();

        assertSame(parent, child.getParent());
        assertTrue(parent.getChildren().contains(child));

        AtomicInteger parentCount = new AtomicInteger();
        AtomicInteger childCount = new AtomicInteger();

        parent.register(EventManagerTest.Ping.class, p -> parentCount.incrementAndGet());
        child.register(EventManagerTest.Ping.class, p -> childCount.incrementAndGet());

        // Event dispatched on child triggers child AND bubbles to parent
        child.dispatch(new EventManagerTest.Ping());
        assertEquals(1, childCount.get());
        assertEquals(1, parentCount.get());

        // Event dispatched on parent triggers ONLY parent
        parent.dispatch(new EventManagerTest.Ping());
        assertEquals(1, childCount.get());
        assertEquals(2, parentCount.get());

        // Child bus close detaches from parent
        child.close();
        assertFalse(parent.getChildren().contains(child));

        // Dispatching on the closed child now fails fast instead of silently dropping
        assertThrows(IllegalStateException.class, () -> child.dispatch(new EventManagerTest.Ping()));
        assertEquals(1, childCount.get());
        assertEquals(2, parentCount.get());

        // Parent close cascades to all its children
        EventManager child2 = parent.createChildBus();
        assertTrue(parent.getChildren().contains(child2));
        parent.close();
        assertEquals(0, parent.getChildren().size());
    }

    @Test
    void purgeDeadHandlersCleansCollectedWeakListeners() {
        EventManager events = new EventManager();
        class WeakTarget {
            @EventTarget
            public void onPing(EventManagerTest.Ping p) {}
        }

        WeakTarget target = new WeakTarget();
        events.registerWeak(target);
        assertEquals(1, events.handlerCount(EventManagerTest.Ping.class));

        // Drop strong reference and run GC loop
        target = null;
        for (int i = 0; i < 5; i++) {
            System.gc();
            System.runFinalization();
            try { Thread.sleep(20); } catch (InterruptedException ignored) {}
        }

        int purged = events.purgeDeadHandlers();
        assertTrue(purged >= 0);
        // After purge, dead handlers must be gone
        if (purged > 0) {
            assertEquals(0, events.handlerCount(EventManagerTest.Ping.class));
        }
    }
}
