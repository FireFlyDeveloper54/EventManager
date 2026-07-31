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

        @Override
        public boolean isSubscribed() {
            return false;
        }
    };

    void unsubscribe();

    /**
     * Returns whether this subscription is still active. Custom implementations that do not track
     * state remain active by default.
     *
     * @return {@code true} while this subscription is active
     */
    default boolean isSubscribed() {
        return true;
    }

    /**
     * Returns a subscription that unsubscribes both this subscription and {@code other}.
     *
     * @param other subscription to combine with this one
     * @return grouped subscription
     */
    default Subscription and(Subscription other) {
        return Subscriptions.combine(this, other);
    }
}
