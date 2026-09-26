package dev.hotaru.event.benchmark;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import dev.hotaru.event.EventManager;
import dev.hotaru.event.impl.Event;
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

/**
 * Head-to-head comparison against Guava EventBus on identical workloads.
 * This is NOT a rigorous cross-library benchmark study — same JVM, same
 * machine, small handler counts — but it grounds the performance claims in
 * something reproducible.
 *
 * Run with:
 *   ./gradlew jmhRun -PjmhArgs="VersusGuavaBenchmark.* -f 1 -wi 3 -i 5"
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class VersusGuavaBenchmark {

    public static final class PingEvent implements Event {}

    public static final class GuavaListener {
        int sink;
        @Subscribe
        public void onPing(PingEvent event) {
            sink++;
        }
        @Subscribe
        public void onPingFallback(Object event) {
            sink += 2;
        }
    }

    private EventManager hotaruSingle;
    private EventManager hotaruTen;
    private EventBus guavaSingle;
    private EventBus guavaTen;
    private GuavaListener guavaSingleListener;
    private PingEvent ping = new PingEvent();

    @Setup
    public void setUp() {
        hotaruSingle = new EventManager();
        hotaruSingle.register(PingEvent.class, new java.util.function.Consumer<PingEvent>() {
            @Override
            public void accept(PingEvent event) {}
        });

        hotaruTen = new EventManager();
        for (int i = 0; i < 10; i++) {
            hotaruTen.register(PingEvent.class, new java.util.function.Consumer<PingEvent>() {
                @Override
                public void accept(PingEvent event) {}
            });
        }

        guavaSingle = new EventBus("guava-single");
        guavaSingleListener = new GuavaListener();
        guavaSingle.register(guavaSingleListener);

        guavaTen = new EventBus("guava-ten");
        for (int i = 0; i < 10; i++) {
            guavaTen.register(new GuavaListener());
        }
    }

    @Benchmark
    public Event hotaruSingleHandler() {
        return hotaruSingle.dispatch(ping);
    }

    @Benchmark
    public void guavaSingleHandler() {
        guavaSingle.post(ping);
    }

    @Benchmark
    public Event hotaruTenHandlers() {
        return hotaruTen.dispatch(ping);
    }

    @Benchmark
    public void guavaTenHandlers() {
        guavaTen.post(ping);
    }
}
