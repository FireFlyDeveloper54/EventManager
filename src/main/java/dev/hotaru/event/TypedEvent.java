package dev.hotaru.event;

/**
 * Base class for events that carry a lifecycle {@link EventType} phase.
 *
 * <p>Moved from {@code dev.hotaru.event.impl.TypedEvent}: the old location
 * remains as a deprecated copy. New code should import this type.
 */
public abstract class TypedEvent implements Event {
    private final EventType type;

    protected TypedEvent(EventType type) {
        this.type = type;
    }

    public EventType getType() {
        return type;
    }

    public boolean isPre() {
        return type == EventType.PRE;
    }

    public boolean isPost() {
        return type == EventType.POST;
    }
}
