package dev.hotaru.event;

public interface Subscription {

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

    default boolean isSubscribed() {
        return true;
    }

    default Subscription and(Subscription other) {
        return Subscriptions.combine(this, other);
    }
}
