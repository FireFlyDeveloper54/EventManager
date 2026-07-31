package com.cubk.event;

import com.cubk.event.impl.Event;

import java.util.function.Consumer;

/**
 * A typed listener that can be registered directly or as an annotated field on a listener object.
 *
 * @param <T> event type handled by this listener
 */
@FunctionalInterface
public interface EventListener<T extends Event> extends Consumer<T> {
    void onEvent(T event);

    /**
     * Returns this listener's default priority. An {@code @EventPriority} annotation on a field
     * overrides this value.
     *
     * @return listener priority, where lower values run first
     */
    default int getPriority() {
        return Priority.NORMAL;
    }

    @Override
    default void accept(T event) {
        onEvent(event);
    }
}
