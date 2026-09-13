package dev.hotaru.event;

import dev.hotaru.event.impl.Event;

/**
 * Wraps a checked throwable when {@link ErrorPolicy#PROPAGATE} is selected.
 */
public final class EventDispatchException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final Event event;
    private final Object listener;

    public EventDispatchException(Event event, Object listener, Throwable cause) {
        super("Event listener failed while dispatching "
                + (event == null ? "<null>" : event.getClass().getName()), cause);
        this.event = event;
        this.listener = listener;
    }

    public EventDispatchException(String message, Event event, Throwable cause) {
        this(message, event, null, cause);
    }

    public EventDispatchException(String message, Event event, Object listener, Throwable cause) {
        super(message, cause);
        this.event = event;
        this.listener = listener;
    }

    public Event getEvent() {
        return event;
    }

    public Object getListener() {
        return listener;
    }
}
