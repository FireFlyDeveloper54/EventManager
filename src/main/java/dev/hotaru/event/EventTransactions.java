package dev.hotaru.event;

import dev.hotaru.event.impl.Event;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Thread-isolated event transaction machinery behind
 * {@link EventManager#transaction(Runnable)}: buffers dispatched events until
 * commit, and runs Saga compensations in LIFO order on rollback. State is
 * bound to the calling thread, so nested transactions join the outermost one.
 */
final class EventTransactions {

    private static final Logger log = Logger.getLogger(EventTransactions.class.getName());

    private static final ThreadLocal<EventTransaction> HOLDER = new ThreadLocal<EventTransaction>();

    private EventTransactions() {
    }

    static EventTransaction current() {
        return HOLDER.get();
    }

    static TransactionContext contextOrNull() {
        return HOLDER.get();
    }

    static void execute(final EventManager bus, final Runnable action) {
        Objects.requireNonNull(action, "action");
        execute(bus, new Supplier<Void>() {
            @Override
            public Void get() {
                action.run();
                return null;
            }
        });
    }

    static <R> R execute(EventManager bus, Supplier<R> action) {
        Objects.requireNonNull(action, "action");
        EventTransaction tx = HOLDER.get();
        boolean isRoot = false;
        if (tx == null) {
            tx = new EventTransaction();
            HOLDER.set(tx);
            isRoot = true;
        }
        tx.depth++;
        try {
            R result = action.get();
            if (isRoot) {
                if (!tx.rolledBack) {
                    List<BufferedEvent> toFlush = new ArrayList<BufferedEvent>(tx.buffer);
                    tx.buffer.clear();
                    HOLDER.remove();
                    // Flush every buffered event even if one of them fails:
                    // a single broken dispatch must not silently drop the rest.
                    // Errors are recorded (never swallowed) and rethrown after
                    // the commit hooks have run. If the bus was closed while the
                    // transaction was in flight, each dispatch throws
                    // IllegalStateException and is collected like any other
                    // flush failure (hooks still run, first error rethrown).
                    Throwable flushError = null;
                    for (BufferedEvent be : toFlush) {
                        try {
                            if (be.sticky) {
                                bus.writeSticky(be.event);
                            }
                            if (be.exact) {
                                bus.dispatchExact(be.event);
                            } else {
                                bus.dispatch(be.event);
                            }
                        } catch (Throwable t) {
                            if (flushError == null) {
                                flushError = t;
                            } else {
                                flushError.addSuppressed(t);
                            }
                        }
                    }
                    for (Runnable commitHook : tx.commitHooks) {
                        try {
                            commitHook.run();
                        } catch (Throwable t) {
                            log.log(Level.WARNING, "Transaction commit hook failed", t);
                        }
                    }
                    if (flushError != null) {
                        if (flushError instanceof RuntimeException) {
                            throw (RuntimeException) flushError;
                        }
                        if (flushError instanceof Error) {
                            throw (Error) flushError;
                        }
                        throw new RuntimeException("Transaction flush failed", flushError);
                    }
                } else {
                    tx.buffer.clear();
                    HOLDER.remove();
                    for (int i = tx.compensations.size() - 1; i >= 0; i--) {
                        try {
                            tx.compensations.get(i).run();
                        } catch (Throwable t) {
                            log.log(Level.SEVERE, "Saga compensation action failed; remaining compensations still run", t);
                        }
                    }
                }
            }
            return result;
        } catch (Throwable t) {
            tx.rolledBack = true;
            if (isRoot) {
                tx.buffer.clear();
                HOLDER.remove();
                for (int i = tx.compensations.size() - 1; i >= 0; i--) {
                    try {
                        tx.compensations.get(i).run();
                    } catch (Throwable error) {
                        log.log(Level.SEVERE, "Saga compensation action failed; remaining compensations still run", error);
                    }
                }
            }
            if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            }
            if (t instanceof Error) {
                throw (Error) t;
            }
            throw new RuntimeException("Transaction aborted due to exception", t);
        } finally {
            if (!isRoot) {
                tx.depth--;
            }
        }
    }

    static void executeWithContext(final EventManager bus, final Consumer<TransactionContext> action) {
        Objects.requireNonNull(action, "action");
        execute(bus, new Supplier<Void>() {
            @Override
            public Void get() {
                action.accept(HOLDER.get());
                return null;
            }
        });
    }

    static final class BufferedEvent {
        final Event event;
        final boolean exact;
        final boolean sticky;

        BufferedEvent(Event event, boolean exact) {
            this(event, exact, false);
        }

        BufferedEvent(Event event, boolean exact, boolean sticky) {
            this.event = event;
            this.exact = exact;
            this.sticky = sticky;
        }
    }

    static final class EventTransaction implements TransactionContext {
        final List<BufferedEvent> buffer = new ArrayList<BufferedEvent>();
        final List<Runnable> compensations = new ArrayList<Runnable>();
        final List<Runnable> commitHooks = new ArrayList<Runnable>();
        int depth = 0;
        boolean rolledBack = false;

        @Override
        public void onRollback(Runnable compensationAction) {
            if (compensationAction != null) {
                compensations.add(compensationAction);
            }
        }

        @Override
        public void onCommit(Runnable commitHook) {
            if (commitHook != null) {
                commitHooks.add(commitHook);
            }
        }

        @Override
        public void rollback() {
            this.rolledBack = true;
        }

        @Override
        public boolean isRollbackOnly() {
            return this.rolledBack;
        }
    }
}
