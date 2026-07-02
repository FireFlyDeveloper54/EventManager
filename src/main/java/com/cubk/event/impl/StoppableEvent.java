package com.cubk.event.impl;

/**
 * Base class for events that can stop further dispatch.
 */
public abstract class StoppableEvent implements Event, Stoppable {
    private boolean stopped;

    @Override
    public void setStopped(boolean state) {
        this.stopped = state;
    }

    @Override
    public boolean isStopped() {
        return stopped;
    }
}
