package com.cubk.event;

import com.cubk.event.impl.Event;

/**
 * Receives exceptions thrown by event handlers. Dispatch continues with the remaining handlers
 * after this is invoked.
 */
public interface EventErrorHandler {
    /**
     * @param event     the event being dispatched
     * @param listener  the listener whose handler threw (for functional listeners, the consumer itself)
     * @param throwable the exception thrown by the handler
     */
    void handle(Event event, Object listener, Throwable throwable);
}
