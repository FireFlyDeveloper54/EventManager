package dev.hotaru.event;

import dev.hotaru.event.impl.Event;

import java.util.Objects;

/**
 * Filter predicate evaluated before dispatching an event to a handler.
 *
 * <p>Implementations should provide a public no-arg constructor if used in
 * {@link dev.hotaru.event.annotations.EventTarget#filter()}.</p>
 *
 * @param <T> the event type to filter
 */
@FunctionalInterface
public interface EventFilter<T extends Event> {

    /**
     * Evaluates this filter on the given event.
     *
     * @param event the event to test
     * @return true if the event should be passed to the handler, false otherwise
     */
    boolean test(T event);

    /**
     * Returns a composed filter that represents a short-circuiting logical AND of this filter and another.
     */
    default EventFilter<T> and(final EventFilter<? super T> other) {
        Objects.requireNonNull(other, "other");
        return event -> test(event) && other.test(event);
    }

    /**
     * Returns a composed filter that represents a short-circuiting logical OR of this filter and another.
     */
    default EventFilter<T> or(final EventFilter<? super T> other) {
        Objects.requireNonNull(other, "other");
        return event -> test(event) || other.test(event);
    }

    /**
     * Returns a filter that represents the logical negation of this filter.
     */
    default EventFilter<T> negate() {
        return event -> !test(event);
    }

    /**
     * Returns a filter that represents the logical negation of the specified filter.
     */
    static <T extends Event> EventFilter<T> not(EventFilter<T> filter) {
        Objects.requireNonNull(filter, "filter");
        return filter.negate();
    }

    /**
     * Converts a {@link java.util.function.Predicate} into an {@link EventFilter}.
     */
    static <T extends Event> EventFilter<T> fromPredicate(final java.util.function.Predicate<T> predicate) {
        Objects.requireNonNull(predicate, "predicate");
        return predicate::test;
    }

    /**
     * Views this event filter as a standard {@link java.util.function.Predicate}.
     */
    default java.util.function.Predicate<T> asPredicate() {
        return this::test;
    }

    /**
     * Default pass-through filter that accepts all events.
     */
    final class PassAll implements EventFilter<Event> {
        @Override
        public boolean test(Event event) {
            return true;
        }
    }
}
