package dev.hotaru.event;

import dev.hotaru.event.impl.Event;

import java.util.Objects;

/**
 * Wraps an event that was dispatched through the event bus but had no active handlers
 * to receive it.
 *
 * <p>Useful for detecting unhandled events, module initialization order issues,
 * and unrouted messages.</p>
 */
public final class DeadEvent implements Event {
    private final Object source;
    private final Event event;
    private final long timestamp;

    private final EventTrace trace;

    public DeadEvent(Object source, Event event) {
        this(source, event, System.currentTimeMillis(), null);
    }

    public DeadEvent(Object source, Event event, EventTrace trace) {
        this(source, event, System.currentTimeMillis(), trace);
    }

    public DeadEvent(Object source, Event event, long timestamp) {
        this(source, event, timestamp, null);
    }

    public DeadEvent(Object source, Event event, long timestamp, EventTrace trace) {
        this.source = Objects.requireNonNull(source, "source");
        this.event = Objects.requireNonNull(event, "event");
        this.timestamp = timestamp;
        this.trace = trace;
    }

    public Object getSource() {
        return source;
    }

    public Event getEvent() {
        return event;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public EventTrace getTrace() {
        return trace;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DeadEvent)) return false;
        DeadEvent that = (DeadEvent) o;
        return timestamp == that.timestamp
                && Objects.equals(source, that.source)
                && Objects.equals(event, that.event);
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, event, timestamp);
    }

    @Override
    public String toString() {
        return "DeadEvent{" +
                "source=" + source +
                ", event=" + event +
                ", timestamp=" + timestamp +
                '}';
    }
}
