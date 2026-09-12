package dev.hotaru.event;

import dev.hotaru.event.impl.Event;

/**
 * Allows a listener object to dynamically enable or disable its event handling
 * at runtime without unregistering from the event bus.
 */
public interface EventSubscriber {

    /**
     * Returns true if this subscriber is currently accepting events.
     * Defaults to true.
     */
    default boolean isHandlingEvents() {
        return true;
    }

    /**
     * Returns true if this subscriber should handle the specified event.
     * Defaults to delegating to {@link #isHandlingEvents()}.
     */
    default boolean isHandlingEvents(Event event) {
        return isHandlingEvents();
    }
}
