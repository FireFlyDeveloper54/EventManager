package dev.hotaru.event.impl;

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
