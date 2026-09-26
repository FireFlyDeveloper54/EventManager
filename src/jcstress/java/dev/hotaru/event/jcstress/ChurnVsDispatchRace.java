package dev.hotaru.event.jcstress;

import dev.hotaru.event.EventManager;
import dev.hotaru.event.Subscription;
import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.I_Result;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * One thread churns an annotated listener (register/unregister loops) while
 * another dispatches repeatedly. The stable observer is registered in the
 * state constructor, i.e. strictly before any actor runs, so it must receive
 * every dispatch exactly once — regardless of the concurrent cache
 * invalidations the churn causes. No CME/NPE may escape either actor.
 *
 * Result is 101 when the observer saw all {@link #DISPATCHES} dispatches and
 * the churn was clean; -1 on any violation.
 */
@JCStressTest
@Outcome(id = "101", expect = Expect.ACCEPTABLE, desc = "Observer got all dispatches exactly once, churn clean")
@Outcome(expect = Expect.FORBIDDEN, desc = "Lost dispatch, duplicate delivery, or churn failure")
public class ChurnVsDispatchRace {

    public static final int DISPATCHES = 10;

    @State
    public static class St {
        final EventManager bus = new EventManager();
        final AtomicInteger obsCount = new AtomicInteger();
        volatile boolean churnFailed = false;

        public St() {
            bus.register(Support.Ping.class, new Consumer<Support.Ping>() {
                @Override
                public void accept(Support.Ping event) {
                    obsCount.incrementAndGet();
                }
            });
        }
    }

    @Actor
    public static void churner(St s) {
        try {
            for (int i = 0; i < 20; i++) {
                Support.AnnotatedListener l = new Support.AnnotatedListener(null);
                s.bus.register(l);
                s.bus.unregister(l);
            }
        } catch (Throwable t) {
            s.churnFailed = true;
        }
    }

    @Actor
    public static void dispatcher(St s) {
        try {
            for (int i = 0; i < DISPATCHES; i++) {
                s.bus.dispatch(new Support.Ping());
            }
        } catch (Throwable t) {
            s.churnFailed = true;
        }
    }

    @Arbiter
    public static void arbiter(St s, I_Result r) {
        int obs = s.obsCount.get();
        r.r1 = (!s.churnFailed && obs == DISPATCHES) ? obs * 10 + 1 : -1;
    }
}
