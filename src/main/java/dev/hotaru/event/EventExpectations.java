package dev.hotaru.event;


import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Async event await machinery behind {@link EventManager#expect(Class)}:
 * completes a {@link CompletableFuture} with the next event matching a filter,
 * with optional timeout and automatic unsubscription whichever way it ends.
 */
final class EventExpectations {

    private EventExpectations() {
    }

    static <T extends Event> CompletableFuture<T> await(
            final EventManager bus,
            final Class<T> eventType,
            final Predicate<? super T> filter,
            final long timeout,
            final TimeUnit unit) {
        Objects.requireNonNull(eventType, "eventType");
        final CompletableFuture<T> future = new CompletableFuture<T>();
        final AtomicReference<Subscription> subRef = new AtomicReference<Subscription>();
        final AtomicReference<ScheduledFuture<?>> timeoutFutureRef =
                new AtomicReference<ScheduledFuture<?>>();

        Subscription subscription = bus.on(eventType)
                .filter(filter)
                .handle(new Consumer<T>() {
                    @Override
                    public void accept(T event) {
                        if (future.complete(event)) {
                            Subscription s = subRef.get();
                            if (s != null) {
                                s.unsubscribe();
                            }
                            ScheduledFuture<?> tf = timeoutFutureRef.get();
                            if (tf != null) {
                                tf.cancel(false);
                            }
                        }
                    }
                });
        subRef.set(subscription);

        if (future.isDone()) {
            subscription.unsubscribe();
            return future;
        }

        if (timeout > 0 && unit != null) {
            ScheduledFuture<?> timeoutTask = TimeoutScheduler.scheduler().schedule(new Runnable() {
                @Override
                public void run() {
                    if (!future.isDone()) {
                        Subscription s = subRef.get();
                        if (s != null) {
                            s.unsubscribe();
                        }
                        future.completeExceptionally(new TimeoutException(
                                "Timed out waiting for event " + eventType.getName() + " after " + timeout + " " + unit));
                    }
                }
            }, timeout, unit);
            timeoutFutureRef.set(timeoutTask);
        }

        future.whenComplete(new java.util.function.BiConsumer<T, Throwable>() {
            @Override
            public void accept(T t, Throwable ex) {
                Subscription s = subRef.get();
                if (s != null) {
                    s.unsubscribe();
                }
                ScheduledFuture<?> tf = timeoutFutureRef.get();
                if (tf != null) {
                    tf.cancel(false);
                }
            }
        });

        return future;
    }
}
