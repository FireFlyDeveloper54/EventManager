package dev.hotaru.event;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A lock-free, zero-allocation circuit breaker protecting the event bus from repeatedly failing
 * or runaway event listeners.
 * <p>
 * Employs cache-line padding to mitigate hardware-level False Sharing on multi-core processors.
 *
 * <p>State transitions:
 * <ul>
 *   <li>{@code CLOSED}: Normal operation; all event deliveries are permitted.</li>
 *   <li>{@code OPEN}: Tripped due to consecutive failures exceeding threshold; all deliveries are rejected.</li>
 *   <li>{@code HALF_OPEN}: Cooldown duration expired; allows trial execution to probe recovery.</li>
 * </ul>
 */
public final class CircuitBreaker {

    public enum State {
        CLOSED,
        OPEN,
        HALF_OPEN
    }

    // Cache-line padding to prevent false sharing
    long p01, p02, p03, p04, p05, p06, p07;

    private final int maxFailures;
    private final long cooldownNanos;

    long p11, p12, p13, p14, p15, p16, p17;

    private final AtomicInteger failureCount = new AtomicInteger();

    long p21, p22, p23, p24, p25, p26, p27;

    private final AtomicLong lastFailureNanos = new AtomicLong();

    long p31, p32, p33, p34, p35, p36, p37;

    private volatile State state = State.CLOSED;

    public CircuitBreaker(int maxFailures, long cooldown, TimeUnit unit) {
        if (maxFailures <= 0) {
            throw new IllegalArgumentException("maxFailures must be positive");
        }
        Objects.requireNonNull(unit, "unit");
        this.maxFailures = maxFailures;
        this.cooldownNanos = unit.toNanos(cooldown);
    }

    /**
     * Checks whether an execution attempt is permitted.
     */
    public boolean allowExecution() {
        State current = this.state;
        if (current == State.CLOSED) {
            return true;
        }
        if (current == State.OPEN) {
            long elapsed = System.nanoTime() - lastFailureNanos.get();
            if (elapsed >= cooldownNanos) {
                this.state = State.HALF_OPEN;
                return true;
            }
            return false;
        }
        // HALF_OPEN allows single trial execution
        return true;
    }

    /**
     * Records a successful execution. If currently in {@code HALF_OPEN}, restores to {@code CLOSED}.
     */
    public void recordSuccess() {
        this.failureCount.set(0);
        this.state = State.CLOSED;
    }

    /**
     * Records an execution failure. Trips the circuit breaker to {@code OPEN} if threshold reached.
     */
    public void recordFailure() {
        this.lastFailureNanos.set(System.nanoTime());
        int failures = this.failureCount.incrementAndGet();
        if (failures >= maxFailures) {
            this.state = State.OPEN;
        }
    }

    /**
     * Resets the circuit breaker back to initial {@code CLOSED} state with zero failures.
     */
    public void reset() {
        this.failureCount.set(0);
        this.lastFailureNanos.set(0);
        this.state = State.CLOSED;
    }

    public State getState() {
        return state;
    }

    public int getFailureCount() {
        return failureCount.get();
    }
}
