package com.cubk.event.impl;

/**
 * Marks an event whose propagation can be stopped.
 */
public interface Stoppable {
    boolean isStopped();

    void setStopped(boolean state);
}
