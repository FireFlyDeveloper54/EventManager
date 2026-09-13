package dev.hotaru.event;

/**
 * Defines strategies for handling overflow when an asynchronous event channel is full.
 */
public enum BackpressurePolicy {
    /**
     * Blocks the calling thread until capacity becomes available in the channel.
     */
    BLOCK,

    /**
     * Drops the oldest pending event in the channel to make room for the new event.
     */
    DROP_OLDEST,

    /**
     * Drops the incoming event if the channel is full, without affecting already queued events.
     */
    DROP_LATEST,

    /**
     * If the channel is full, executes the event dispatch synchronously on the caller's thread.
     */
    CALLER_RUNS
}
