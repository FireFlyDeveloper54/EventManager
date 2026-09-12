package dev.hotaru.event;

import java.util.Objects;

/** Immutable snapshot of event-bus dispatch counters and latency profile. */
public final class EventMetrics {
    private final Class<?> eventType;
    private final long dispatchedEvents;
    private final long handlerInvocations;
    private final long failures;
    private final long totalDurationNanos;
    private final long maxDurationNanos;

    public EventMetrics(long dispatchedEvents, long handlerInvocations, long failures) {
        this(null, dispatchedEvents, handlerInvocations, failures, 0L, 0L);
    }

    public EventMetrics(Class<?> eventType, long dispatchedEvents,
                        long handlerInvocations, long failures) {
        this(eventType, dispatchedEvents, handlerInvocations, failures, 0L, 0L);
    }

    public EventMetrics(long dispatchedEvents, long handlerInvocations, long failures,
                        long totalDurationNanos, long maxDurationNanos) {
        this(null, dispatchedEvents, handlerInvocations, failures, totalDurationNanos, maxDurationNanos);
    }

    public EventMetrics(Class<?> eventType, long dispatchedEvents,
                        long handlerInvocations, long failures,
                        long totalDurationNanos, long maxDurationNanos) {
        this.eventType = eventType;
        this.dispatchedEvents = dispatchedEvents;
        this.handlerInvocations = handlerInvocations;
        this.failures = failures;
        this.totalDurationNanos = totalDurationNanos;
        this.maxDurationNanos = maxDurationNanos;
    }

    public Class<?> getEventType() {
        return eventType;
    }

    public long getDispatchedEvents() {
        return dispatchedEvents;
    }

    public long getHandlerInvocations() {
        return handlerInvocations;
    }

    public long getFailures() {
        return failures;
    }

    public long getTotalDurationNanos() {
        return totalDurationNanos;
    }

    public long getMaxDurationNanos() {
        return maxDurationNanos;
    }

    public double getAverageDurationNanos() {
        return dispatchedEvents == 0 ? 0.0 : (double) totalDurationNanos / dispatchedEvents;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EventMetrics)) return false;
        EventMetrics that = (EventMetrics) o;
        return dispatchedEvents == that.dispatchedEvents
                && handlerInvocations == that.handlerInvocations
                && failures == that.failures
                && totalDurationNanos == that.totalDurationNanos
                && maxDurationNanos == that.maxDurationNanos
                && Objects.equals(eventType, that.eventType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventType, dispatchedEvents, handlerInvocations, failures, totalDurationNanos, maxDurationNanos);
    }

    @Override
    public String toString() {
        return "EventMetrics{"
                + (eventType != null ? "eventType=" + eventType.getName() + ", " : "")
                + "dispatchedEvents=" + dispatchedEvents
                + ", handlerInvocations=" + handlerInvocations
                + ", failures=" + failures
                + ", totalDurationNanos=" + totalDurationNanos
                + ", maxDurationNanos=" + maxDurationNanos
                + ", avgDurationNanos=" + String.format(java.util.Locale.ROOT, "%.2f", getAverageDurationNanos())
                + '}';
    }
}
