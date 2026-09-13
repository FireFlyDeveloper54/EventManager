package dev.hotaru.event;

import dev.hotaru.event.impl.Event;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * A reactive event publisher representing a typed stream of events from the event bus.
 *
 * @param <T> the event type
 */
public interface EventPublisher<T extends Event> {

    /**
     * Returns the class of event emitted by this publisher.
     */
    Class<T> getEventType();

    /**
     * Subscribes a consumer to receive events from this stream.
     *
     * @param subscriber the event consumer
     * @return a subscription handle to cancel the stream
     */
    Subscription subscribe(Consumer<? super T> subscriber);

    /**
     * Subscribes a consumer with an additional filter predicate.
     *
     * @param filter     the filter to apply to emitted events
     * @param subscriber the event consumer
     * @return a subscription handle to cancel the stream
     */
    Subscription subscribe(Predicate<? super T> filter, Consumer<? super T> subscriber);

    /**
     * Returns a new publisher that filters events matching the specified predicate.
     */
    default EventPublisher<T> filter(final Predicate<? super T> filter) {
        Objects.requireNonNull(filter, "filter");
        final EventPublisher<T> self = this;
        return new EventPublisher<T>() {
            @Override
            public Class<T> getEventType() {
                return self.getEventType();
            }

            @Override
            public Subscription subscribe(final Consumer<? super T> subscriber) {
                return self.subscribe(filter, subscriber);
            }

            @Override
            public Subscription subscribe(final Predicate<? super T> nextFilter, final Consumer<? super T> subscriber) {
                return self.subscribe(new Predicate<T>() {
                    @Override
                    public boolean test(T t) {
                        return filter.test(t) && (nextFilter == null || nextFilter.test(t));
                    }
                }, subscriber);
            }
        };
    }
}
