package dev.hotaru.event;

import dev.hotaru.event.impl.Event;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Utility methods for composing and chaining {@link EventInterceptor} instances.
 */
public final class EventInterceptors {

    private EventInterceptors() {
    }

    /**
     * Chains two interceptors sequentially: {@code first} executes first, and calls
     * {@code second}, which in turn calls the actual dispatch handler.
     *
     * @param first the outer interceptor
     * @param second the inner interceptor
     * @return the combined interceptor, or null if both are null
     */
    public static EventInterceptor chain(final EventInterceptor first, final EventInterceptor second) {
        if (first == null) {
            return second;
        }
        if (second == null) {
            return first;
        }
        return new EventInterceptor() {
            @Override
            public void intercept(final Event event, final Runnable proceed) {
                first.intercept(event, new Runnable() {
                    @Override
                    public void run() {
                        second.intercept(event, proceed);
                    }
                });
            }
        };
    }

    /**
     * Chains an array of interceptors in sequential order.
     *
     * @param interceptors the interceptors to chain
     * @return the combined interceptor, or null if the array is empty or all elements are null
     */
    public static EventInterceptor chain(EventInterceptor... interceptors) {
        if (interceptors == null || interceptors.length == 0) {
            return null;
        }
        EventInterceptor combined = null;
        for (int i = 0; i < interceptors.length; i++) {
            EventInterceptor next = interceptors[i];
            if (next != null) {
                combined = chain(combined, next);
            }
        }
        return combined;
    }

    /**
     * Chains an iterable collection of interceptors in sequential order.
     *
     * @param interceptors the collection of interceptors
     * @return the combined interceptor, or null if empty or all elements are null
     */
    public static EventInterceptor chain(Iterable<? extends EventInterceptor> interceptors) {
        if (interceptors == null) {
            return null;
        }
        EventInterceptor combined = null;
        for (EventInterceptor next : interceptors) {
            if (next != null) {
                combined = chain(combined, next);
            }
        }
        return combined;
    }
}
