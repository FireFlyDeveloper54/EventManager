package dev.hotaru.event;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public final class Subscriptions {
    private Subscriptions() {}

    /**
     * Creates a subscription that executes the specified runnable exactly once upon unsubscription.
     */
    public static Subscription of(final Runnable action) {
        if (action == null) {
            return Subscription.NOOP;
        }
        return new Subscription() {
            private final AtomicBoolean executed = new AtomicBoolean(false);

            @Override
            public void unsubscribe() {
                if (executed.compareAndSet(false, true)) {
                    action.run();
                }
            }

            @Override
            public boolean isSubscribed() {
                return !executed.get();
            }
        };
    }

    /**
     * Combines an iterable collection of subscriptions into a composite subscription.
     */
    public static Subscription combine(Iterable<? extends Subscription> subscriptions) {
        if (subscriptions == null) {
            return Subscription.NOOP;
        }
        List<Subscription> active = new ArrayList<Subscription>();
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
            Throwable firstFailure = null;
            for (Subscription subscription : subscriptions) {
                try {
                    subscription.unsubscribe();
                } catch (Throwable failure) {
                    if (firstFailure == null) {
                        firstFailure = failure;
                    } else {
                        firstFailure.addSuppressed(failure);
                    }
                }
            }
            if (firstFailure instanceof RuntimeException) {
                throw (RuntimeException) firstFailure;
            }
            if (firstFailure instanceof Error) {
                throw (Error) firstFailure;
            }
            if (firstFailure != null) {
                throw new IllegalStateException("Subscription cleanup failed", firstFailure);
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
