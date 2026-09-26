package dev.hotaru.event;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Shared daemon scheduler for infrastructure timeouts: {@code expect()} await
 * deadlines and SubscriberBuilder debounce/buffer flushes. A single shared
 * thread is intentional — callbacks only cancel work and complete futures,
 * never run user listener code.
 */
final class TimeoutScheduler {

    private static final class Holder {
        private static final ScheduledExecutorService SCHEDULER = Executors.newSingleThreadScheduledExecutor(
                new java.util.concurrent.ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, "EventManager-TimeoutScheduler");
                        t.setDaemon(true);
                        return t;
                    }
                }
        );
    }

    private TimeoutScheduler() {
    }

    static ScheduledExecutorService scheduler() {
        return Holder.SCHEDULER;
    }
}
