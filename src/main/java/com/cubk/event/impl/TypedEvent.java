package com.cubk.event.impl;

/**
 * Base class for events that fire in multiple phases, such as before and after a workflow step,
 * so one event class can serve every phase.
 */
public abstract class TypedEvent implements Event {
    private final EventType type;

    /**
     * @param type event phase
     */
    protected TypedEvent(EventType type) {
        this.type = type;
    }

    /**
     * @return event phase
     */
    public EventType getType() {
        return type;
    }

    /**
     * @return whether this is the pre phase
     */
    public boolean isPre() {
        return type == EventType.PRE;
    }

    /**
     * @return whether this is the post phase
     */
    public boolean isPost() {
        return type == EventType.POST;
    }
}
