package dev.hotaru.event;

public interface EventSubscriber {

    default boolean isHandlingEvents() {
        return true;
    }
}
