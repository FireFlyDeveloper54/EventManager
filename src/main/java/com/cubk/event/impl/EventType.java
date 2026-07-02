package com.cubk.event.impl;

/**
 * Common phases for events that fire around another action, e.g. pre/post update or message send/receive.
 * Domain-specific phases should live in your own enum on a custom event base class instead of here.
 */
public enum EventType {
    PRE,
    MID,
    POST,
    SEND,
    RECEIVE
}
