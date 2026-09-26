package dev.hotaru.event.jcstress;

import dev.hotaru.event.EventManager;
import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.I_Result;

/**
 * Two threads register the SAME annotated listener instance concurrently.
 * The second registration must be absorbed by Handler-equals dedup inside
 * the CAS insert loop, regardless of interleaving — so the event type ends
 * up with exactly one handler.
 */
@JCStressTest
@Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "Exactly one handler registered")
@Outcome(id = "2", expect = Expect.FORBIDDEN, desc = "Duplicate registration raced through")
@Outcome(id = "0", expect = Expect.FORBIDDEN, desc = "Registration lost")
public class SameListenerRegistrationRace {

    @State
    public static class St {
        final EventManager bus = new EventManager();
        final Support.AnnotatedListener listener = new Support.AnnotatedListener(null);
    }

    @Actor
    public static void actor1(St s) {
        s.bus.register(s.listener);
    }

    @Actor
    public static void actor2(St s) {
        s.bus.register(s.listener);
    }

    @Arbiter
    public static void arbiter(St s, I_Result r) {
        r.r1 = s.bus.exactHandlerCount(Support.Ping.class);
    }
}
