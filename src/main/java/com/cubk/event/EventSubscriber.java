package com.cubk.event;

/**
 * Optional contract for listener objects that can temporarily disable their own handlers without
 * unregistering from the event manager.
 */
public interface EventSubscriber {
    /**
     * @return {@code true} while this subscriber should receive events
     */
    default boolean isHandlingEvents() {
        return true;
    }
}
