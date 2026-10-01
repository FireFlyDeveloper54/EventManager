package dev.hotaru.event;

/**
 * Lifecycle phase of a {@link TypedEvent}.
 *
 * <p>Moved from {@code dev.hotaru.event.impl.EventType}: the old location
 * remains as a deprecated duplicate. New code should import this type.
 */
public enum EventType {
    PRE,
    MID,
    POST,
    SEND,
    RECEIVE
}
