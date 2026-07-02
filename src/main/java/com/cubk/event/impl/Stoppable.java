package com.cubk.event.impl;

/**
 * Marks an event whose propagation can be stopped.
 */
public interface Stoppable {
    boolean isStopped();

    void setStopped(boolean state);

    /**
     * Stops further event dispatch.
     */
    default void stop() {
        setStopped(true);
    }

    /**
     * Allows dispatch to continue again if the event is reused before dispatch ends.
     */
    default void resume() {
        setStopped(false);
    }
}
