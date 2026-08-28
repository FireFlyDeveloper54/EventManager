package dev.hotaru.event.impl;

public abstract class CancellableStoppableEvent extends CancellableEvent implements Stoppable {
    private boolean stopped;
    private boolean stoppedByCancel;

    @Override
    public boolean isStopped() {
        return stopped;
    }

    @Override
    public void setStopped(boolean state) {
        this.stopped = state;
        this.stoppedByCancel = false;
    }

    @Override
    public void setCancelled(boolean state) {
        super.setCancelled(state);
        if (state) {
            if (!stopped) {
                this.stoppedByCancel = true;
            }
            this.stopped = true;
        } else if (stoppedByCancel) {
            this.stoppedByCancel = false;
            this.stopped = false;
        }
    }
}
