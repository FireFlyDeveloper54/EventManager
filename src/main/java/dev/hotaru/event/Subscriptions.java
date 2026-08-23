package dev.hotaru.event;

import lombok.RequiredArgsConstructor;
import lombok.experimental.UtilityClass;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@UtilityClass
public class Subscriptions {
    public Subscription combine(Subscription... subscriptions) {
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

    private boolean containsIdentity(List<Subscription> subscriptions, Subscription candidate) {
        for (Subscription subscription : subscriptions) {
            if (subscription == candidate) {
                return true;
            }
        }
        return false;
    }

    @RequiredArgsConstructor
    private static final class CompositeSubscription implements Subscription {
        private final Subscription[] subscriptions;
        private final AtomicBoolean subscribed = new AtomicBoolean(true);

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
