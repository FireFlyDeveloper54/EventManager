package dev.hotaru.event.impl;

/**
 * @deprecated Moved to {@link dev.hotaru.event.TypedEvent}. This is a frozen
 * deprecated copy kept only for source and binary compatibility of existing
 * code (it cannot extend the new type because its constructor takes the old
 * {@link EventType}).
 */
@Deprecated
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
