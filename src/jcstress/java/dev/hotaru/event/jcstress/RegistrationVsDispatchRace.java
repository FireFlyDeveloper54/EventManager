package dev.hotaru.event.jcstress;

import dev.hotaru.event.EventManager;
import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.I_Result;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * One actor registers a consumer while another dispatches concurrently; the
 * arbiter then registers a fresh consumer and dispatches again.
 *
 * Invariants: a handler registered before a dispatch receives that dispatch
 * exactly once (no duplicates), and its total delivery count over both
 * dispatches is 1 or 2 depending on whether it won the racing snapshot.
 * Two-actor shape keeps the test schedulable at -c 2.
 *
 * Result encodes deliveries as 10*firstConsumer + secondConsumer.
 */
@JCStressTest
@Outcome(id = "11", expect = Expect.ACCEPTABLE, desc = "Racer missed phase-1 snapshot, both exact once after")
@Outcome(id = "21", expect = Expect.ACCEPTABLE, desc = "Racer won phase-1 snapshot, both exact once after")
@Outcome(expect = Expect.FORBIDDEN, desc = "Duplicate or lost delivery")
public class RegistrationVsDispatchRace {

    @State
    public static class St {
        final EventManager bus = new EventManager();
        final AtomicInteger xCount = new AtomicInteger();
        final AtomicInteger yCount = new AtomicInteger();
    }

    @Actor
    public static void racer(St s) {
        s.bus.register(Support.Ping.class, new Consumer<Support.Ping>() {
            @Override
            public void accept(Support.Ping event) {
                s.xCount.incrementAndGet();
            }
        });
    }

    @Actor
    public static void dispatcher(St s) {
        s.bus.dispatch(new Support.Ping());
    }

    @Arbiter
    public static void arbiter(St s, I_Result r) {
        s.bus.register(Support.Ping.class, new Consumer<Support.Ping>() {
            @Override
            public void accept(Support.Ping event) {
                s.yCount.incrementAndGet();
            }
        });
        s.bus.dispatch(new Support.Ping());
        int x = s.xCount.get();
        int y = s.yCount.get();
        r.r1 = (y == 1 && x >= 1 && x <= 2) ? x * 10 + y : -1;
    }
}
