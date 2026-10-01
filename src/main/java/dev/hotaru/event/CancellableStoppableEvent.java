package dev.hotaru.event;

/**
 * Convenient base class for events that are both cancellable and stoppable.
 * Cancelling also stops propagation; explicitly un-stopping clears the
 * cancel-driven stop.
 *
 * <p>Moved from {@code dev.hotaru.event.impl.CancellableStoppableEvent}: the
 * old location remains as a deprecated alias. New code should import this type.
 */
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
