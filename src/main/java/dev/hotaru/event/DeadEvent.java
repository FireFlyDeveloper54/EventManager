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

    public DeadEvent(Object source, Event event) {
        this(source, event, System.currentTimeMillis());
    }

    public DeadEvent(Object source, Event event, long timestamp) {
        this.source = Objects.requireNonNull(source, "source");
        this.event = Objects.requireNonNull(event, "event");
        this.timestamp = timestamp;
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
