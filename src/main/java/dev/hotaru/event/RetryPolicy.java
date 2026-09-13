package dev.hotaru.event;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Encapsulates retry behavior, backoff strategies, and exception predicates for listener actions.
 */
public final class RetryPolicy {

    private final int maxAttempts;
    private final long initialDelayNanos;
    private final double backoffMultiplier;
    private final long maxDelayNanos;
    private final Predicate<Throwable> retryCondition;

    public RetryPolicy(int maxAttempts, long initialDelay, TimeUnit unit, double multiplier, long maxDelay, Predicate<Throwable> retryCondition) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1");
        }
        Objects.requireNonNull(unit, "unit");
        this.maxAttempts = maxAttempts;
        this.initialDelayNanos = unit.toNanos(initialDelay);
        this.backoffMultiplier = multiplier < 1.0 ? 1.0 : multiplier;
        this.maxDelayNanos = unit.toNanos(maxDelay);
        this.retryCondition = retryCondition != null ? retryCondition : new Predicate<Throwable>() {
            @Override
            public boolean test(Throwable t) {
                return true;
            }
        };
    }

    public static RetryPolicy fixed(int maxAttempts, long delay, TimeUnit unit) {
        return new RetryPolicy(maxAttempts, delay, unit, 1.0, delay, null);
    }

    public static RetryPolicy exponentialBackoff(int maxAttempts, long initialDelay, long maxDelay, TimeUnit unit) {
        return new RetryPolicy(maxAttempts, initialDelay, unit, 2.0, maxDelay, null);
    }

    public static RetryPolicy noRetry() {
        return new RetryPolicy(1, 0, TimeUnit.MILLISECONDS, 1.0, 0, new Predicate<Throwable>() {
            @Override
            public boolean test(Throwable t) {
                return false;
            }
        });
    }

    public RetryPolicy retryOn(final Class<? extends Throwable> exceptionType) {
        Objects.requireNonNull(exceptionType, "exceptionType");
        final Predicate<Throwable> prev = this.retryCondition;
        return new RetryPolicy(maxAttempts, initialDelayNanos, TimeUnit.NANOSECONDS, backoffMultiplier, maxDelayNanos,
                new Predicate<Throwable>() {
                    @Override
                    public boolean test(Throwable t) {
                        return prev.test(t) && exceptionType.isInstance(t);
                    }
                });
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public long getDelayNanosForAttempt(int attempt) {
        if (attempt <= 1 || initialDelayNanos <= 0) {
            return 0L;
        }
        double delay = initialDelayNanos * Math.pow(backoffMultiplier, attempt - 2);
        long delayNanos = (long) delay;
        if (maxDelayNanos > 0 && delayNanos > maxDelayNanos) {
            return maxDelayNanos;
        }
        return delayNanos;
    }

    public boolean canRetry(Throwable t, int attempt) {
        return attempt < maxAttempts && retryCondition.test(t);
    }
}
