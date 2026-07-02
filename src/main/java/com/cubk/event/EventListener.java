package com.cubk.event;

import com.cubk.event.impl.Event;

import java.util.function.Consumer;

/**
 * A typed listener that can be registered as an annotated field on a listener object.
 *
 * @param <T> event type handled by this listener
 */
@FunctionalInterface
public interface EventListener<T extends Event> extends Consumer<T> {
    void onEvent(T event);

    @Override
    default void accept(T event) {
        onEvent(event);
    }
}
