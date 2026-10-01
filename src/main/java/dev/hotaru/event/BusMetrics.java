package dev.hotaru.event;

import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Dispatch metrics for a single {@link EventManager} bus.
 *
 * <p>Extracted from {@code EventManager} to keep the bus itself a thin
 * facade. All counters are lock-free; per-type counters use weak keys so
 * metrics bookkeeping never pins an event class (or its class loader) after
 * the type becomes unreachable.
 *
 * <p>This class is intentionally package-private: the public surface stays on
 * {@link EventManager#metrics()}, {@link EventManager#metrics(Class)},
 * {@link EventManager#resetMetrics()} and the enabled flag accessors.
 */
final class BusMetrics {

    private final LongAdder dispatchedEvents = new LongAdder();
    private final LongAdder handlerInvocations = new LongAdder();
    private final LongAdder failures = new LongAdder();
    private final LongAdder totalDurationNanos = new LongAdder();
    private final AtomicLong maxDurationNanos = new AtomicLong();

    // Weak keys so per-type metrics never pin an event class (or its class
    // loader) after the type becomes unreachable. Compound check-then-act
    // sequences synchronize on this map explicitly; see counterFor.
    private final Map<Class<?>, MetricCounter> metricsByType =
            Collections.synchronizedMap(new WeakHashMap<Class<?>, MetricCounter>());

    private volatile boolean enabled = true;

    boolean isEnabled() {
        return enabled;
    }

    void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    void recordDispatch(Class<?> eventType) {
        dispatchedEvents.increment();
        MetricCounter counter = counterFor(eventType);
        if (counter != null) counter.dispatchedEvents.increment();
    }

    void recordInvocation(Class<?> eventType) {
        handlerInvocations.increment();
        MetricCounter counter = counterFor(eventType);
        if (counter != null) counter.handlerInvocations.increment();
    }

    void recordFailure(Class<?> eventType) {
        failures.increment();
        MetricCounter counter = counterFor(eventType);
        if (counter != null) counter.failures.increment();
    }

    void recordDuration(Class<?> eventType, long nanos) {
        totalDurationNanos.add(nanos);
        long currentMax;
        while (nanos > (currentMax = maxDurationNanos.get())) {
            if (maxDurationNanos.compareAndSet(currentMax, nanos)) {
                break;
            }
        }
        MetricCounter counter = counterFor(eventType);
        if (counter != null) {
            counter.recordDuration(nanos);
        }
    }

    /** Returns a lock-free snapshot of dispatch counters and latency profile. */
    EventMetrics snapshot() {
        return new EventMetrics(null, dispatchedEvents.sum(), handlerInvocations.sum(),
                failures.sum(), totalDurationNanos.sum(), maxDurationNanos.get());
    }

    /** Returns counters for one runtime event type without retaining its class loader forever. */
    EventMetrics snapshot(Class<? extends Event> eventType) {
        if (eventType == null) {
            return new EventMetrics(null, 0L, 0L, 0L, 0L, 0L);
        }
        MetricCounter counter = metricsByType.get(eventType);
        return counter == null
                ? new EventMetrics(eventType, 0L, 0L, 0L, 0L, 0L)
                : counter.snapshot(eventType);
    }

    /** Resets dispatch counters without changing registrations. */
    void reset() {
        dispatchedEvents.reset();
        handlerInvocations.reset();
        failures.reset();
        totalDurationNanos.reset();
        maxDurationNanos.set(0L);
        metricsByType.clear();
    }

    private MetricCounter counterFor(Class<?> eventType) {
        if (eventType == null) {
            return null;
        }
        // The check-then-act sequence must be atomic: the synchronized map
        // wrapper alone does not make putIfAbsent atomic, so two racing
        // threads could otherwise create and publish two counters for the
        // same type and lose increments.
        synchronized (metricsByType) {
            MetricCounter counter = metricsByType.get(eventType);
            if (counter == null) {
                counter = new MetricCounter();
                metricsByType.put(eventType, counter);
            }
            return counter;
        }
    }

    static final class MetricCounter {
        private final LongAdder dispatchedEvents = new LongAdder();
        private final LongAdder handlerInvocations = new LongAdder();
        private final LongAdder failures = new LongAdder();
        private final LongAdder totalDurationNanos = new LongAdder();
        private final AtomicLong maxDurationNanos = new AtomicLong();

        private void recordDuration(long nanos) {
            totalDurationNanos.add(nanos);
            long currentMax;
            while (nanos > (currentMax = maxDurationNanos.get())) {
                if (maxDurationNanos.compareAndSet(currentMax, nanos)) {
                    break;
                }
            }
        }

        private EventMetrics snapshot(Class<?> eventType) {
            return new EventMetrics(eventType, dispatchedEvents.sum(),
                    handlerInvocations.sum(), failures.sum(),
                    totalDurationNanos.sum(), maxDurationNanos.get());
        }
    }
}
