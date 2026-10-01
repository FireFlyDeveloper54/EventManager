package dev.hotaru.event;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Cache of the latest sticky event per concrete event type.
 *
 * <p>Extracted from {@link EventManager} to keep the bus itself a thin
 * facade. Newly registered sticky listeners immediately receive the cached
 * event for their type upon registration (see
 * {@link EventManager#replaySticky}).
 *
 * <p>This class is intentionally package-private: the public surface stays on
 * {@link EventManager#dispatchSticky}, {@link EventManager#getSticky},
 * {@link EventManager#removeSticky} and {@link EventManager#clearSticky}.
 */
final class StickyEventStore {

    private final ConcurrentMap<Class<? extends Event>, Event> stickyEvents =
            new ConcurrentHashMap<Class<? extends Event>, Event>();

    private final UpcasterRegistry upcasters;

    StickyEventStore(UpcasterRegistry upcasters) {
        this.upcasters = upcasters;
    }

    /**
     * Caches the event under its concrete runtime type, including the
     * upcaster cascade: converted events are cached under their own types
     * as well.
     */
    void write(Event event) {
        stickyEvents.put(event.getClass(), event);
        cascadeStickyUpcasting(event);
    }

    private void cascadeStickyUpcasting(Event event) {
        List<EventUpcaster.Typed<?, ?>> upcasterList = upcasters.forType(event.getClass());
        if (upcasterList != null && !upcasterList.isEmpty()) {
            for (EventUpcaster.Typed<?, ?> upcaster : upcasterList) {
                @SuppressWarnings("unchecked")
                EventUpcaster.Typed<Object, Object> typed = (EventUpcaster.Typed<Object, Object>) upcaster;
                Object upcasted = typed.upcast(event);
                if (upcasted instanceof Event) {
                    Event upcastedEvent = (Event) upcasted;
                    stickyEvents.put(upcastedEvent.getClass(), upcastedEvent);
                    cascadeStickyUpcasting(upcastedEvent);
                }
            }
        }
    }

    /**
     * Returns the latest cached sticky event of the specified class, or
     * {@code null} if none exists. Falls back to the first cached event
     * whose concrete type is assignable to {@code eventType}.
     */
    <T extends Event> T get(Class<T> eventType) {
        if (eventType == null) {
            return null;
        }
        Event event = stickyEvents.get(eventType);
        if (event == null) {
            for (Map.Entry<Class<? extends Event>, Event> entry : stickyEvents.entrySet()) {
                if (eventType.isAssignableFrom(entry.getKey())) {
                    return eventType.cast(entry.getValue());
                }
            }
        }
        return eventType.cast(event);
    }

    /** Removes and returns the cached sticky event of the specified class. */
    <T extends Event> T remove(Class<T> eventType) {
        if (eventType == null) {
            return null;
        }
        Event event = stickyEvents.remove(eventType);
        return eventType.cast(event);
    }

    /** Returns a snapshot of cached events that are instances of {@code targetType}. */
    List<Event> matching(Class<?> targetType) {
        List<Event> result = new ArrayList<Event>();
        for (Event sticky : stickyEvents.values()) {
            if (targetType.isInstance(sticky)) {
                result.add(sticky);
            }
        }
        return result;
    }

    boolean isEmpty() {
        return stickyEvents.isEmpty();
    }

    void clear() {
        stickyEvents.clear();
    }
}
