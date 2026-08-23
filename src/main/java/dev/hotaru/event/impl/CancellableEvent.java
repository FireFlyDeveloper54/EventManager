package dev.hotaru.event.impl;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public abstract class CancellableEvent implements Event, Cancellable {
    private boolean cancelled;
}
