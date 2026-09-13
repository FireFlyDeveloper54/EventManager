package dev.hotaru.event;

import dev.hotaru.event.impl.Event;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Immutable session containing recorded events from an {@link EventRecorder}.
 */
public final class RecordedSession {
    private final List<RecordedEvent> events;
    private final long durationNanos;

    public RecordedSession(List<RecordedEvent> events, long durationNanos) {
        this.events = Collections.unmodifiableList(events);
        this.durationNanos = durationNanos;
    }

    public List<RecordedEvent> getEvents() {
        return events;
    }

    public int size() {
        return events.size();
    }

    public long getDurationNanos() {
        return durationNanos;
    }

    public void replayTo(EventManager targetBus) {
        replayTo(targetBus, false);
    }

    public void replayTo(EventManager targetBus, boolean preserveDelays) {
        Objects.requireNonNull(targetBus, "targetBus");
        long prevRelative = 0L;
        for (RecordedEvent recorded : events) {
            if (preserveDelays && recorded.getRelativeNanos() > prevRelative) {
                long sleepNanos = recorded.getRelativeNanos() - prevRelative;
                try {
                    TimeUnit.NANOSECONDS.sleep(sleepNanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            prevRelative = recorded.getRelativeNanos();
            if (recorded.getContext() != null) {
                targetBus.dispatch(recorded.getEvent(), recorded.getContext());
            } else {
                targetBus.dispatch(recorded.getEvent());
            }
        }
    }
}
