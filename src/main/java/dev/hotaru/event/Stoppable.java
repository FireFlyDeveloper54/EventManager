package dev.hotaru.event;

/**
 * An event whose propagation can be stopped. A stopped event is not delivered
 * to remaining handlers and does not bubble to a parent bus.
 *
 * <p>Moved from {@code dev.hotaru.event.impl.Stoppable}: the old location
 * remains as a deprecated alias. New code should import this type.
 */
public interface Stoppable {

    boolean isStopped();

    void setStopped(boolean state);

    default void stop() {
        setStopped(true);
    }

    default void resume() {
        setStopped(false);
    }
}
