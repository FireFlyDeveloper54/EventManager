package dev.hotaru.event.impl;

/**
 * @deprecated Moved to {@link dev.hotaru.event.EventType}. Enums cannot be
 * aliased by inheritance, so this is a frozen deprecated duplicate kept only
 * for source and binary compatibility of existing code.
 */
@Deprecated
public enum EventType {
    PRE,
    MID,
    POST,
    SEND,
    RECEIVE
}
