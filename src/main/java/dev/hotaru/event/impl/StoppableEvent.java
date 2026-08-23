package dev.hotaru.event.impl;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public abstract class StoppableEvent implements Event, Stoppable {
    private boolean stopped;
}
