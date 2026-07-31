package com.cubk.event;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Utilities for combining subscription lifecycles.
 */
public final class Subscriptions {
    private Subscriptions() {
    }

    /**
     * Combines multiple subscriptions into one idempotent subscription.
     *
     * @param subscriptions subscriptions to combine
     * @return grouped subscription, or {@link Subscription#NOOP} when none are usable
     */
    public static Subscription combine(Subscription... subscriptions) {
        if (subscriptions == null || subscriptions.length == 0) {
            return Subscription.NOOP;
        }

        List<Subscription> active = new ArrayList<Subscription>(subscriptions.length);
        for (Subscription subscription : subscriptions) {
            if (subscription != null
                    && subscription != Subscription.NOOP
                    && !containsIdentity(active, subscription)) {
                active.add(subscription);
            }
        }

        if (active.isEmpty()) {
            return Subscription.NOOP;
        }
        return new CompositeSubscription(active.toArray(new Subscription[active.size()]));
    }

    private static boolean containsIdentity(List<Subscription> subscriptions, Subscription candidate) {
        for (Subscription subscription : subscriptions) {
            if (subscription == candidate) {
                return true;
            }
        }
        return false;
    }

    private static final class CompositeSubscription implements Subscription {
        private final Subscription[] subscriptions;
        private final AtomicBoolean subscribed = new AtomicBoolean(true);

        private CompositeSubscription(Subscription[] subscriptions) {
            this.subscriptions = subscriptions;
        }

        @Override
        public void unsubscribe() {
            if (!subscribed.compareAndSet(true, false)) {
                return;
            }
            for (Subscription subscription : subscriptions) {
                subscription.unsubscribe();
            }
        }

        @Override
        public boolean isSubscribed() {
            if (!subscribed.get()) {
                return false;
            }
            for (Subscription subscription : subscriptions) {
                if (subscription.isSubscribed()) {
                    return true;
                }
            }
            return false;
        }
    }
}
