package dev.hotaru.event.impl;

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
