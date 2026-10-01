package dev.hotaru.event;


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
     * <p>
     * The {@code proceed} callback must be invoked <b>at most once</b>; invoking
     * it a second time throws {@link IllegalStateException}. It should be invoked
     * synchronously before this method returns: retaining the callback and
     * invoking it afterwards is not supported and may interact badly with the
     * internal frame pooling.
     *
     * @param event   The event currently being dispatched.
     * @param proceed A callback to continue the standard downstream dispatch to listeners.
     *                If this callback is not invoked, listener dispatch is skipped.
     */
    void intercept(Event event, Runnable proceed);
}
