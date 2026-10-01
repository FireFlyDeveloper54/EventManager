package dev.hotaru.event;

/**
 * An event that can be cancelled. Once cancelled, parent-bus bubbling and
 * upcaster propagation stop for it.
 *
 * <p>Moved from {@code dev.hotaru.event.impl.Cancellable}: the old location
 * remains as a deprecated alias. New code should import this type.
 */
public interface Cancellable {
    boolean isCancelled();

    void setCancelled(boolean state);

    default void cancel() {
        setCancelled(true);
    }

    default void uncancel() {
        setCancelled(false);
    }
}
