package dev.hotaru.event;


@FunctionalInterface
public interface EventErrorHandler {
    void handle(Event event, Object listener, Throwable throwable);
}
