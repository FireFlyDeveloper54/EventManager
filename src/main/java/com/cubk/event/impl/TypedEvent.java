package com.cubk.event.impl;

/**
 * Base class for events that fire in multiple phases, such as before and after a workflow step,
 * so one event class can serve every phase.
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
