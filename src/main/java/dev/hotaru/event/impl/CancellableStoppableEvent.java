package dev.hotaru.event.impl;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public abstract class CancellableStoppableEvent extends CancellableEvent implements Stoppable {
    private boolean stopped;

    @Override
    public void setCancelled(boolean cancelled) {
        super.setCancelled(cancelled);
        if (cancelled) {
            this.stopped = true;
        }
    }
}
