package dev.hotaru.event;

/**
 * Convenient base class for cancellable events.
 *
 * <p>Moved from {@code dev.hotaru.event.impl.CancellableEvent}: the old
 * location remains as a deprecated alias. New code should import this type.
 */
public abstract class CancellableEvent implements Event, Cancellable {
    private boolean cancelled;

    @Override
    public boolean isCancelled() {
        return cancelled;
    }

    @Override
    public void setCancelled(boolean cancelled) {
        this.cancelled = cancelled;
    }
}
