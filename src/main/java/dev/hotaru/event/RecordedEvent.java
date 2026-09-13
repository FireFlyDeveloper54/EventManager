package dev.hotaru.event;

import dev.hotaru.event.impl.Event;
import java.util.Objects;

/**
 * Represents a single captured event in an {@link EventRecorder} session.
 */
public final class RecordedEvent {
    private final Event event;
    private final long timestampNanos;
    private final long relativeNanos;
    private final EventContext context;

    public RecordedEvent(Event event, long timestampNanos, long relativeNanos, EventContext context) {
        this.event = Objects.requireNonNull(event, "event");
        this.timestampNanos = timestampNanos;
        this.relativeNanos = relativeNanos;
        this.context = context;
    }

    public Event getEvent() {
        return event;
    }

    public <T extends Event> T getEvent(Class<T> type) {
        return type.cast(event);
    }

    public long getTimestampNanos() {
        return timestampNanos;
    }

    public long getRelativeNanos() {
        return relativeNanos;
    }

    public EventContext getContext() {
        return context;
    }

    @Override
    public String toString() {
        return "RecordedEvent{" + event.getClass().getSimpleName() + ", relativeMs=" + (relativeNanos / 1000000.0) + "}";
    }
}
