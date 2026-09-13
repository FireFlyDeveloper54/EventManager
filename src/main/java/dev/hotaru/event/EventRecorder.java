package dev.hotaru.event;

import dev.hotaru.event.impl.Event;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

/**
 * Non-invasive event recorder capturing event dispatches and relative timestamps for deterministic testing and replay.
 */
public final class EventRecorder implements AutoCloseable {
    private final EventManager bus;
    private final Predicate<Event> filter;
    private final long startNanos;
    private final List<RecordedEvent> recordedList = new CopyOnWriteArrayList<RecordedEvent>();
    private final AtomicBoolean recording = new AtomicBoolean(true);
    private final EventInterceptor interceptor;

    EventRecorder(final EventManager bus, final Predicate<Event> filter) {
        this.bus = Objects.requireNonNull(bus, "bus");
        this.filter = filter;
        this.startNanos = System.nanoTime();

        this.interceptor = new EventInterceptor() {
            @Override
            public void intercept(Event event, Runnable proceed) {
                if (recording.get() && (filter == null || filter.test(event))) {
                    long now = System.nanoTime();
                    recordedList.add(new RecordedEvent(event, now, now - startNanos, EventContext.current()));
                }
                proceed.run();
            }
        };
        bus.addInterceptor(this.interceptor);
    }

    public RecordedSession stop() {
        if (recording.compareAndSet(true, false)) {
            bus.removeInterceptor(interceptor);
        }
        return new RecordedSession(recordedList, System.nanoTime() - startNanos);
    }

    public boolean isRecording() {
        return recording.get();
    }

    public int getRecordedCount() {
        return recordedList.size();
    }

    @Override
    public void close() {
        stop();
    }
}
