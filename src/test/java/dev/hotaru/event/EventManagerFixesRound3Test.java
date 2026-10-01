package dev.hotaru.event;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Regression tests for the third review round: closed-bus semantics.
 * Every behavior-producing entry point must fail fast with
 * IllegalStateException on a closed bus, while cleanup and query
 * operations stay usable.
 */
public class EventManagerFixesRound3Test {

    private static Consumer<EventManagerTest.Ping> pingConsumer() {
        return new Consumer<EventManagerTest.Ping>() {
            @Override
            public void accept(EventManagerTest.Ping ping) {
            }
        };
    }

    // Closed bus: dispatch/dispatchExact fail fast instead of silently dropping.
    @Test
    void closedBusDispatchThrows() {
        EventManager bus = new EventManager();
        bus.close();
        assertThrows(IllegalStateException.class,
                () -> bus.dispatch(new EventManagerTest.Ping()));
        assertThrows(IllegalStateException.class,
                () -> bus.dispatchExact(new EventManagerTest.Ping()));
    }

    // Closed bus: dispatchSticky fails before touching the sticky cache.
    @Test
    void closedBusDispatchStickyThrows() {
        EventManager bus = new EventManager();
        bus.close();
        assertThrows(IllegalStateException.class,
                () -> bus.dispatchSticky(new EventManagerTest.Ping()));
        assertNull(bus.getSticky(EventManagerTest.Ping.class),
                "closed bus must not write the sticky cache");
    }

    // Closed bus: every registration path funnels through addHandler's check.
    @Test
    void closedBusRegisterThrows() {
        EventManager bus = new EventManager();
        bus.close();
        assertThrows(IllegalStateException.class,
                () -> bus.register(EventManagerTest.Ping.class, pingConsumer()));
        assertThrows(IllegalStateException.class,
                () -> bus.registerOnce(EventManagerTest.Ping.class, pingConsumer()));
        assertThrows(IllegalStateException.class,
                () -> bus.on(EventManagerTest.Ping.class).handle(pingConsumer()));
        assertThrows(IllegalStateException.class,
                () -> bus.registerUpcaster(
                        EventManagerTest.Ping.class,
                        EventManagerTest.AdminPing.class,
                        new Function<EventManagerTest.Ping, EventManagerTest.AdminPing>() {
                            @Override
                            public EventManagerTest.AdminPing apply(EventManagerTest.Ping ping) {
                                return new EventManagerTest.AdminPing();
                            }
                        }));
        assertThrows(IllegalStateException.class,
                () -> bus.addInterceptor(new EventInterceptor() {
                    @Override
                    public void intercept(Event event, Runnable proceed) {
                        proceed.run();
                    }
                }));
    }

    // Closed bus: starting a transaction is fail-fast, not silently buffered.
    @Test
    void closedBusTransactionThrows() {
        EventManager bus = new EventManager();
        bus.close();
        assertThrows(IllegalStateException.class,
                () -> bus.transaction(new Consumer<TransactionContext>() {
                    @Override
                    public void accept(TransactionContext tx) {
                    }
                }));
    }

    // Closed bus: cleanup and query operations stay available.
    @Test
    void closedBusCleanupStillAllowed() {
        EventManager bus = new EventManager();
        bus.register(EventManagerTest.Ping.class, pingConsumer());
        bus.close();
        bus.clear();
        bus.unregisterEventType(EventManagerTest.Ping.class);
        bus.removeEntry(EventManagerTest.Ping.class);
        bus.purgeDeadHandlers();
        bus.resetMetrics();
        assertNull(bus.getSticky(EventManagerTest.Ping.class));
        assertTrue(bus.isClosed());
    }

    // Null dispatches stay null-tolerant even on a closed bus (no behavior produced).
    @Test
    void closedBusDispatchNullReturnsNull() {
        EventManager bus = new EventManager();
        bus.close();
        assertNull(bus.dispatch(null));
        assertNull(bus.dispatchExact(null));
        assertNull(bus.dispatchSticky(null));
    }

    // Bubbling into a closed parent fails fast with IllegalStateException.
    // The child is built directly (not via createChildBus) so closing the
    // parent does not cascade-close the child under test.
    @Test
    void childBubblesToClosedParentThrows() {
        EventManager parent = new EventManager();
        EventManager child = new EventManager("child", parent, null, null, null);
        parent.close();
        assertTrue(!child.isClosed(), "test setup: child must stay open");
        assertThrows(IllegalStateException.class,
                () -> child.dispatch(new EventManagerTest.Ping()));
    }

    // Async dispatch on a closed bus: the failure surfaces through the future.
    @Test
    void closedBusDispatchAsyncCompletesExceptionally() throws Exception {
        EventManager bus = new EventManager();
        bus.close();
        CompletableFuture<EventManagerTest.Ping> future =
                bus.dispatchAsync(new EventManagerTest.Ping());
        try {
            future.get(5, TimeUnit.SECONDS);
            fail("expected ExecutionException");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof IllegalStateException,
                    "async dispatch on a closed bus must fail with IllegalStateException, got "
                            + e.getCause());
        }
    }

    // Bus closed mid-transaction: flush dispatches hit the closed bus, the
    // IllegalStateException is collected like any flush failure, commit
    // hooks still run, and the error propagates to the caller.
    @Test
    void closeDuringTransactionFlushPropagatesAndRunsHooks() {
        final EventManager bus = new EventManager();
        final AtomicBoolean hookRan = new AtomicBoolean(false);
        try {
            bus.transaction(new Consumer<TransactionContext>() {
                @Override
                public void accept(TransactionContext tx) {
                    tx.onCommit(new Runnable() {
                        @Override
                        public void run() {
                            hookRan.set(true);
                        }
                    });
                    bus.dispatch(new EventManagerTest.Ping());
                    bus.close();
                }
            });
            fail("flush on a closed bus must propagate");
        } catch (IllegalStateException e) {
            // expected: flush-time dispatch hit the closed bus
        }
        assertTrue(hookRan.get(), "commit hooks must still run when flush fails");
    }
}
