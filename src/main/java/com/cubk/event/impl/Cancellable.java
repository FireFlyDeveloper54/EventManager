package com.cubk.event.impl;

/**
 * An interface for events carrying an application-defined cancellation result.
 *
 * <p>Cancellation commonly asks the event publisher to suppress an associated default action.
 * The event manager only exposes the state to handlers and applies {@code ignoreCancelled}; it
 * does not otherwise interpret what cancellation means for the application.
 */
public interface Cancellable {

    /**
     * Checks if the object is cancelled.
     *
     * @return {@code true} if the object is cancelled, {@code false} otherwise.
     */
    boolean isCancelled();

    /**
     * Sets the cancellation state of the object.
     *
     * @param state {@code true} to cancel the object, {@code false} to uncancel it.
     */
    void setCancelled(boolean state);

    /**
     * Marks this object as cancelled. The event publisher decides which default action, if any,
     * should be suppressed as a result.
     */
    default void cancel() {
        setCancelled(true);
    }

    /**
     * Marks this object as not cancelled.
     */
    default void uncancel() {
        setCancelled(false);
    }
}
