package dev.hotaru.event;


import java.util.function.Consumer;

@FunctionalInterface
public interface EventListener<T extends Event> extends Consumer<T> {
    void onEvent(T event);

    default int getPriority() {
        return Priority.NORMAL;
    }

    @Override
    default void accept(T event) {
        onEvent(event);
    }
}
