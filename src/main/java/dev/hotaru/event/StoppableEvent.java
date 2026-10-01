package dev.hotaru.event;

/**
 * Convenient base class for stoppable events.
 *
 * <p>Moved from {@code dev.hotaru.event.impl.StoppableEvent}: the old
 * location remains as a deprecated alias. New code should import this type.
 */
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
