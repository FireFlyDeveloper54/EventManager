package dev.hotaru.event.benchmark;

import dev.hotaru.event.EventManager;
import dev.hotaru.event.Subscription;
import dev.hotaru.event.Event;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Measures the dynamic-subscription pattern: churn subscriptions between
 * dispatches. Every register/unregister invalidates dispatch cache entries,
 * so this exposes the cost of the cache invalidation strategy.
 *
 * Run with:
 *   ./gradlew jmhRun -PjmhArgs="SubscriptionChurnBenchmark.* -f 1 -wi 3 -i 5"
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class SubscriptionChurnBenchmark {

    public static final class TickEvent implements Event {}

    private EventManager bus;
    private TickEvent tick = new TickEvent();
    private Subscription[] subs = new Subscription[8];
    private final Consumer<TickEvent> noop = new Consumer<TickEvent>() {
        @Override
        public void accept(TickEvent event) {}
    };

    @Setup
    public void setUp() {
        bus = new EventManager();
    }

    /** Subscribe 8 consumers, dispatch once, unsubscribe all. One full churn cycle per op. */
    @Benchmark
    public Event subscribeDispatchUnsubscribe() {
        for (int i = 0; i < subs.length; i++) {
            subs[i] = bus.subscribe(noop, TickEvent.class);
        }
        bus.dispatch(tick);
        for (int i = 0; i < subs.length; i++) {
            subs[i].unsubscribe();
        }
        return tick;
    }

    /** Steady dispatch interleaved with a single subscribe/unsubscribe pair per op. */
    @Benchmark
    public Event dispatchWithChurn() {
        Subscription sub = bus.subscribe(noop, TickEvent.class);
        bus.dispatch(tick);
        sub.unsubscribe();
        return tick;
    }
}
