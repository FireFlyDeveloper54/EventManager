package dev.hotaru.event;

import dev.hotaru.event.impl.Event;

/**
 * Functional interface for intercepting event dispatches across the event bus.
 * <p>
 * Interceptors can be used for logging, metrics, timing, security checks,
 * or short-circuiting downstream listener execution.
 */
@FunctionalInterface
public interface EventInterceptor {

    /**
     * Intercepts the dispatch of an event.
     *
     * @param event   The event currently being dispatched.
     * @param proceed A callback to continue the standard downstream dispatch to listeners.
     *                If this callback is not invoked, listener dispatch is skipped.
     */
    void intercept(Event event, Runnable proceed);
}
