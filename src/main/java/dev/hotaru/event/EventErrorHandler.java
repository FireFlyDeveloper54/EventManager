package dev.hotaru.event;

import dev.hotaru.event.impl.Event;

@FunctionalInterface
public interface EventErrorHandler {
    void handle(Event event, Object listener, Throwable throwable);
}
