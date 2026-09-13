package dev.hotaru.event;

import dev.hotaru.event.impl.Event;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 * A high-performance bounded asynchronous event channel with configurable backpressure strategies.
 * <p>
 * Backpressure policies:
 * <ul>
 *   <li>{@link BackpressurePolicy#BLOCK}: Blocks the publisher when full until space becomes available.</li>
 *   <li>{@link BackpressurePolicy#DROP_OLDEST}: Discards the oldest unconsumed event in the buffer.</li>
 *   <li>{@link BackpressurePolicy#DROP_LATEST}: Rejects and discards the new incoming event.</li>
 *   <li>{@link BackpressurePolicy#CALLER_RUNS}: Synchronously dispatches on the caller's thread when saturated.</li>
 * </ul>
 *
 * @param <T> the event type
 */
public final class AsyncEventChannel<T extends Event> implements AutoCloseable {

    private final EventManager bus;
    private final Class<T> eventType;
    private final int capacity;
    private final BackpressurePolicy policy;
    private final BlockingQueue<T> queue;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Thread workerThread;

    private final LongAdder publishedCount = new LongAdder();
    private final LongAdder dispatchedCount = new LongAdder();
    private final LongAdder droppedCount = new LongAdder();

    public AsyncEventChannel(EventManager bus, Class<T> eventType, int capacity, BackpressurePolicy policy) {
        this.bus = Objects.requireNonNull(bus, "bus");
        this.eventType = Objects.requireNonNull(eventType, "eventType");
        this.policy = Objects.requireNonNull(policy, "policy");
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive, got: " + capacity);
        }
        this.capacity = capacity;
        this.queue = new ArrayBlockingQueue<T>(capacity);

        this.workerThread = new Thread(new Worker(), "EventManager-ChannelWorker-" + eventType.getSimpleName());
        this.workerThread.setDaemon(true);
        this.workerThread.start();
    }

    /**
     * Publishes an event to this channel using the configured {@link BackpressurePolicy}.
     *
     * @param event the event to publish
     * @return {@code true} if the event was enqueued or dispatched, {@code false} if dropped
     */
    public boolean publish(T event) {
        Objects.requireNonNull(event, "event");
        if (closed.get()) {
            throw new IllegalStateException("AsyncEventChannel is closed");
        }

        publishedCount.increment();

        switch (policy) {
            case BLOCK: {
                try {
                    while (!closed.get()) {
                        if (queue.offer(event, 50, TimeUnit.MILLISECONDS)) {
                            return true;
                        }
                    }
                    if (closed.get()) {
                        droppedCount.increment();
                        return false;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    droppedCount.increment();
                    return false;
                }
                return true;
            }
            case DROP_OLDEST: {
                while (!queue.offer(event)) {
                    T discarded = queue.poll();
                    if (discarded != null) {
                        droppedCount.increment();
                    }
                }
                return true;
            }
            case DROP_LATEST: {
                if (queue.offer(event)) {
                    return true;
                } else {
                    droppedCount.increment();
                    return false;
                }
            }
            case CALLER_RUNS: {
                if (queue.offer(event)) {
                    return true;
                } else {
                    // Fallback to synchronous dispatch on caller thread
                    bus.dispatch(event);
                    dispatchedCount.increment();
                    return true;
                }
            }
            default:
                throw new UnsupportedOperationException("Unknown policy: " + policy);
        }
    }

    /**
     * Publishes an event with a timeout if {@link BackpressurePolicy#BLOCK} is used.
     */
    public boolean publish(T event, long timeout, TimeUnit unit) throws InterruptedException {
        Objects.requireNonNull(event, "event");
        Objects.requireNonNull(unit, "unit");
        if (closed.get()) {
            throw new IllegalStateException("AsyncEventChannel is closed");
        }

        if (policy != BackpressurePolicy.BLOCK) {
            return publish(event);
        }

        publishedCount.increment();
        if (queue.offer(event, timeout, unit)) {
            return true;
        } else {
            droppedCount.increment();
            return false;
        }
    }

    public int getCapacity() {
        return capacity;
    }

    public int getPendingCount() {
        return queue.size();
    }

    public long getPublishedCount() {
        return publishedCount.sum();
    }

    public long getDispatchedCount() {
        return dispatchedCount.sum();
    }

    public long getDroppedCount() {
        return droppedCount.sum();
    }

    public BackpressurePolicy getPolicy() {
        return policy;
    }

    public Class<T> getEventType() {
        return eventType;
    }

    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            workerThread.interrupt();
            try {
                workerThread.join(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private final class Worker implements Runnable {
        @Override
        public void run() {
            while (!closed.get() || !queue.isEmpty()) {
                try {
                    T event = queue.poll(100, TimeUnit.MILLISECONDS);
                    if (event != null) {
                        try {
                            bus.dispatch(event);
                        } finally {
                            dispatchedCount.increment();
                        }
                    }
                } catch (InterruptedException e) {
                    // check loop conditions
                    if (closed.get() && queue.isEmpty()) {
                        break;
                    }
                } catch (Throwable t) {
                    // prevent worker thread death from unhandled dispatch exceptions
                }
            }
        }
    }
}
