package dev.hotaru.event;

/**
 * Controls what happens after a listener or event-state callback throws.
 */
public enum ErrorPolicy {
    /** Report the failure and continue with the next handler (the default). */
    CONTINUE,
    /** Report the failure and stop the current event dispatch. */
    STOP,
    /** Report the failure and throw it back to the caller. */
    PROPAGATE
}
