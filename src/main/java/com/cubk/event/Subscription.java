package com.cubk.event;

/**
 * A handle to a functional listener registration, used to remove the listener again.
 */
public interface Subscription {
    /**
     * A subscription that does nothing; returned when registration was rejected (null arguments).
     */
    Subscription NOOP = new Subscription() {
        @Override
        public void unsubscribe() {
        }
    };

    void unsubscribe();
}
