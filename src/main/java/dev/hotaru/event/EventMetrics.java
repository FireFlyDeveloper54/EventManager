package dev.hotaru.event;

/** Immutable snapshot of event-bus dispatch counters. */
public final class EventMetrics {
    private final Class<?> eventType;
    private final long dispatchedEvents;
    private final long handlerInvocations;
    private final long failures;

    public EventMetrics(long dispatchedEvents, long handlerInvocations, long failures) {
        this(null, dispatchedEvents, handlerInvocations, failures);
    }

    public EventMetrics(Class<?> eventType, long dispatchedEvents,
                        long handlerInvocations, long failures) {
        this.eventType = eventType;
        this.dispatchedEvents = dispatchedEvents;
        this.handlerInvocations = handlerInvocations;
        this.failures = failures;
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

    @Override
    public String toString() {
        return "EventMetrics{dispatchedEvents=" + dispatchedEvents
                + ", handlerInvocations=" + handlerInvocations
                + ", failures=" + failures + '}';
    }
}
