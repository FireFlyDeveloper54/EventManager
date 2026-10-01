package dev.hotaru.event.benchmark;

import dev.hotaru.event.EventManager;
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
 * Steady-state dispatch throughput benchmarks.
 *
 * Run with:
 *   ./gradlew jmhRun -PjmhArgs="DispatchBenchmark.* -f 1 -wi 3 -i 5"
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class DispatchBenchmark {

    public static final class PingEvent implements Event {}
    public static class ParentEvent implements Event {}
    public static final class ChildEvent extends ParentEvent {}

    private EventManager singleHandlerBus;
    private EventManager tenHandlerBus;
    private EventManager hierarchyBus;
    private PingEvent ping = new PingEvent();
    private ChildEvent child = new ChildEvent();

    @Setup
    public void setUp() {
        singleHandlerBus = new EventManager();
        singleHandlerBus.register(PingEvent.class, new Consumer<PingEvent>() {
            @Override
            public void accept(PingEvent event) {}
        });

        tenHandlerBus = new EventManager();
        for (int i = 0; i < 10; i++) {
            tenHandlerBus.register(PingEvent.class, new Consumer<PingEvent>() {
                @Override
                public void accept(PingEvent event) {}
            });
        }

        hierarchyBus = new EventManager();
        hierarchyBus.register(ParentEvent.class, new Consumer<ParentEvent>() {
            @Override
            public void accept(ParentEvent event) {}
        });
        hierarchyBus.register(ChildEvent.class, new Consumer<ChildEvent>() {
            @Override
            public void accept(ChildEvent event) {}
        });
        // Warm the dispatch cache once so the benchmark measures the cached path.
        hierarchyBus.dispatch(child);
    }

    @Benchmark
    public Event dispatchSingleHandler() {
        return singleHandlerBus.dispatch(ping);
    }

    @Benchmark
    public Event dispatchTenHandlers() {
        return tenHandlerBus.dispatch(ping);
    }

    @Benchmark
    public Event dispatchCachedHierarchy() {
        return hierarchyBus.dispatch(child);
    }
}
