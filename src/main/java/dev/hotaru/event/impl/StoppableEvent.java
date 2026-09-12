package dev.hotaru.event.impl;

public abstract class StoppableEvent implements Event, Stoppable {
    private boolean stopped;

    @Override
    public boolean isStopped() {
        return stopped;
    }

    @Override
    public void setStopped(boolean stopped) {
        this.stopped = stopped;
    }
}
