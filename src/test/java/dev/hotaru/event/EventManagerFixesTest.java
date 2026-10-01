package dev.hotaru.event;

import dev.hotaru.event.impl.Event;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Regression tests for the six review fixes (concurrency + transaction semantics).
 */
public class EventManagerFixesTest {
    // =========================================================================
    // Regression tests for the six review fixes below.
    // =========================================================================

    static final class FlushFirst implements Event {
    }

    static final class FlushSecond implements Event {
    }

    // Fix 1: once handlers must fire exactly once under concurrent dispatch.
    @Test
    void onceHandlerFiresExactlyOnceUnderConcurrentDispatch() throws Exception {
        final int rounds = 20;
        final int threads = 8;
        for (int round = 0; round < rounds; round++) {
            final EventManager bus = new EventManager();
            final AtomicInteger invocations = new AtomicInteger();
            bus.registerOnce(EventManagerTest.Ping.class, new Consumer<EventManagerTest.Ping>() {
                @Override
                public void accept(EventManagerTest.Ping ping) {
                    invocations.incrementAndGet();
                }
            });
            final CountDownLatch start = new CountDownLatch(1);
            final CountDownLatch done = new CountDownLatch(threads);
            for (int i = 0; i < threads; i++) {
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            start.await();
                            bus.dispatch(new EventManagerTest.Ping());
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            done.countDown();
                        }
                    }
                }).start();
            }
            start.countDown();
            assertTrue(done.await(10, TimeUnit.SECONDS), "dispatch threads did not finish");
            assertEquals(1, invocations.get(),
                    "once handler must fire exactly once (round " + round + ")");
            bus.close();
        }
    }

    // Fix 2a: a sticky dispatch inside a rolled-back transaction must not leak
    // into the sticky cache.
    @Test
    void stickyDispatchInTransactionDiscardedOnRollback() {
        final EventManager bus = new EventManager();
        bus.transaction(new Consumer<TransactionContext>() {
            @Override
            public void accept(TransactionContext tx) {
                bus.dispatchSticky(new EventManagerTest.Ping());
                tx.rollback();
            }
        });
        assertNull(bus.getSticky(EventManagerTest.Ping.class), "rolled-back sticky event must not be cached");
        final AtomicInteger replayed = new AtomicInteger();
        bus.on(EventManagerTest.Ping.class).sticky().handle(new Consumer<EventManagerTest.Ping>() {
            @Override
            public void accept(EventManagerTest.Ping ping) {
                replayed.incrementAndGet();
            }
        });
        assertEquals(0, replayed.get(), "rolled-back sticky event must not be replayed");
    }

    // Fix 2b: a sticky dispatch inside a committed transaction is applied.
    @Test
    void stickyDispatchInTransactionAppliedOnCommit() {
        final EventManager bus = new EventManager();
        bus.transaction(new Consumer<TransactionContext>() {
            @Override
            public void accept(TransactionContext tx) {
                bus.dispatchSticky(new EventManagerTest.Ping());
            }
        });
        assertNotNull(bus.getSticky(EventManagerTest.Ping.class), "committed sticky event must be cached");
        final AtomicInteger received = new AtomicInteger();
        bus.on(EventManagerTest.Ping.class).sticky().handle(new Consumer<EventManagerTest.Ping>() {
            @Override
            public void accept(EventManagerTest.Ping ping) {
                received.incrementAndGet();
            }
        });
        assertEquals(1, received.get(), "committed sticky event must be replayed to late subscribers");
    }

    // Fix 3: a handler failure during transaction flush must not drop the
    // remaining events, must not skip commit hooks, and must propagate.
    @Test
    void transactionFlushContinuesAfterHandlerFailure() {
        final AtomicBoolean hookRan = new AtomicBoolean(false);
        final AtomicInteger secondReceived = new AtomicInteger();
        EventManager bus = new EventManager(new EventErrorHandler() {
            @Override
            public void handle(Event event, Object listener, Throwable throwable) {
            }
        }, ErrorPolicy.PROPAGATE);
        bus.register(FlushFirst.class, new Consumer<FlushFirst>() {
            @Override
            public void accept(FlushFirst event) {
                throw new IllegalStateException("boom");
            }
        });
        bus.register(FlushSecond.class, new Consumer<FlushSecond>() {
            @Override
            public void accept(FlushSecond event) {
                secondReceived.incrementAndGet();
            }
        });
        RuntimeException thrown = null;
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
                    bus.dispatch(new FlushFirst());
                    bus.dispatch(new FlushSecond());
                }
            });
            fail("flush failure must propagate to the caller");
        } catch (RuntimeException e) {
            thrown = e;
        }
        assertNotNull(thrown);
        assertTrue(thrown instanceof IllegalStateException);
        assertEquals(1, secondReceived.get(), "events after the failure must still flush");
        assertTrue(hookRan.get(), "commit hooks must run even when flush fails");
    }

    // Fix 4: upcaster registrations that would create a cycle are rejected.
    @Test
    void upcasterCycleRejectedAtRegistration() {
        final EventManager bus = new EventManager();
        bus.registerUpcaster(EventManagerTest.Ping.class, EventManagerTest.AdminPing.class,
                new java.util.function.Function<EventManagerTest.Ping, EventManagerTest.AdminPing>() {
                    @Override
                    public EventManagerTest.AdminPing apply(EventManagerTest.Ping ping) {
                        return new EventManagerTest.AdminPing();
                    }
                });
        assertThrows(IllegalArgumentException.class, () -> bus.registerUpcaster(
                EventManagerTest.AdminPing.class, EventManagerTest.Ping.class,
                new java.util.function.Function<EventManagerTest.AdminPing, EventManagerTest.Ping>() {
                    @Override
                    public EventManagerTest.Ping apply(EventManagerTest.AdminPing ping) {
                        return new EventManagerTest.Ping();
                    }
                }));
        assertThrows(IllegalArgumentException.class, () -> bus.registerUpcaster(
                EventManagerTest.Ping.class, EventManagerTest.Ping.class,
                new java.util.function.Function<EventManagerTest.Ping, EventManagerTest.Ping>() {
                    @Override
                    public EventManagerTest.Ping apply(EventManagerTest.Ping ping) {
                        return ping;
                    }
                }));
        // The legal upcaster still works and dispatch terminates.
        final AtomicInteger upcasted = new AtomicInteger();
        bus.register(EventManagerTest.AdminPing.class, new Consumer<EventManagerTest.AdminPing>() {
            @Override
            public void accept(EventManagerTest.AdminPing ping) {
                upcasted.incrementAndGet();
            }
        });
        bus.dispatch(new EventManagerTest.Ping());
        assertEquals(1, upcasted.get());
    }

    // Fix 5a: upcasters still run and bubble to the parent when the child bus
    // has no handlers.
    @Test
    void upcasterStillBubblesToParentWhenNoHandlers() {
        final EventManager parent = new EventManager();
        final EventManager child = new EventManager(parent);
        final AtomicInteger parentReceived = new AtomicInteger();
        parent.register(EventManagerTest.AdminPing.class, new Consumer<EventManagerTest.AdminPing>() {
            @Override
            public void accept(EventManagerTest.AdminPing ping) {
                parentReceived.incrementAndGet();
            }
        });
        child.registerUpcaster(EventManagerTest.Ping.class, EventManagerTest.AdminPing.class,
                new java.util.function.Function<EventManagerTest.Ping, EventManagerTest.AdminPing>() {
                    @Override
                    public EventManagerTest.AdminPing apply(EventManagerTest.Ping ping) {
                        return new EventManagerTest.AdminPing();
                    }
                });
        child.dispatch(new EventManagerTest.Ping());
        assertEquals(1, parentReceived.get(),
                "upcasted event must bubble to the parent even without local handlers");
    }

    // Fix 5b: a cancelled event does not bubble to the parent, even when the
    // child bus has no handlers.
    @Test
    void cancelledEventDoesNotBubbleToParentWhenNoHandlers() {
        final EventManager parent = new EventManager();
        final EventManager child = new EventManager(parent);
        final AtomicInteger parentReceived = new AtomicInteger();
        parent.register(EventManagerTest.Save.class, new Consumer<EventManagerTest.Save>() {
            @Override
            public void accept(EventManagerTest.Save save) {
                parentReceived.incrementAndGet();
            }
        });
        EventManagerTest.Save save = new EventManagerTest.Save();
        save.setCancelled(true);
        child.dispatch(save);
        assertEquals(0, parentReceived.get(), "cancelled event must not bubble to the parent");
    }

    // Fix 6: concurrent dispatches collapse into a single debounced invocation.
    @Test
    void debounceFiresOnceUnderConcurrentDispatch() throws Exception {
        final EventManager bus = new EventManager();
        final AtomicInteger invocations = new AtomicInteger();
        final CountDownLatch fired = new CountDownLatch(1);
        bus.on(EventManagerTest.Ping.class).debounce(200, TimeUnit.MILLISECONDS).handle(new Consumer<EventManagerTest.Ping>() {
            @Override
            public void accept(EventManagerTest.Ping ping) {
                invocations.incrementAndGet();
                fired.countDown();
            }
        });
        final int threads = 8;
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threads);
        for (int i = 0; i < threads; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        start.await();
                        bus.dispatch(new EventManagerTest.Ping());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                }
            }).start();
        }
        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS), "dispatch threads did not finish");
        assertTrue(fired.await(5, TimeUnit.SECONDS), "debounced handler never fired");
        Thread.sleep(600);
        assertEquals(1, invocations.get(),
                "debounce must collapse concurrent dispatches into one call");
    }

}
