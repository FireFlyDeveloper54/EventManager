package dev.hotaru.event;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Registry of typed event upcasters for automatic schema version migration.
 *
 * <p>Extracted from {@link EventManager} to keep the bus itself a thin
 * facade. Upcasters are applied after dispatch: each converted event is
 * re-dispatched through the owning bus via the dispatch function supplied to
 * {@link #applyUpcasters}.
 *
 * <p>This class is intentionally package-private: the public surface stays on
 * {@link EventManager#registerUpcaster(EventUpcaster.Typed)} and
 * {@link EventManager#registerUpcaster(Class, Class, java.util.function.Function)}.
 */
final class UpcasterRegistry {

    private final ConcurrentMap<Class<?>, List<EventUpcaster.Typed<?, ?>>> upcasters =
            new ConcurrentHashMap<Class<?>, List<EventUpcaster.Typed<?, ?>>>();
    // Hot-path guard: most buses never register an upcaster; this flag lets
    // dispatch skip the per-event map lookup entirely in that common case.
    private volatile boolean hasUpcasters = false;

    /**
     * Registers a typed upcaster, rejecting conversions that would create a
     * cycle. The caller is responsible for the null check and the
     * closed-bus guard.
     */
    <S, T> void register(EventUpcaster.Typed<S, T> upcaster) {
        Class<?> source = upcaster.getSourceType();
        Class<?> target = upcaster.getTargetType();
        if (source != null && target != null && reachesUpcasterTarget(target, source)) {
            throw new IllegalArgumentException("Registering upcaster " + source.getName()
                    + " -> " + target.getName() + " would create an upcaster cycle");
        }
        List<EventUpcaster.Typed<?, ?>> list = upcasters.get(source);
        if (list == null) {
            list = new CopyOnWriteArrayList<EventUpcaster.Typed<?, ?>>();
            List<EventUpcaster.Typed<?, ?>> existing = upcasters.putIfAbsent(source, list);
            if (existing != null) {
                list = existing;
            }
        }
        list.add(upcaster);
        hasUpcasters = true;
    }

    /**
     * Returns the upcasters registered for the event's runtime type, or
     * {@code null} when none apply. The hot-path flag avoids the map lookup
     * entirely for buses without upcasters.
     */
    List<EventUpcaster.Typed<?, ?>> forType(Class<?> eventClass) {
        return hasUpcasters ? upcasters.get(eventClass) : null;
    }

    /**
     * Applies the registered upcasters for the event's runtime type,
     * dispatching each converted event through {@code dispatch}.
     */
    void applyUpcasters(Event event, List<EventUpcaster.Typed<?, ?>> upcasterList, Consumer<Event> dispatch) {
        if (upcasterList == null || upcasterList.isEmpty()) {
            return;
        }
        for (EventUpcaster.Typed<?, ?> upcaster : upcasterList) {
            @SuppressWarnings("unchecked")
            EventUpcaster.Typed<Object, Object> typed = (EventUpcaster.Typed<Object, Object>) upcaster;
            Object upcasted = typed.upcast(event);
            if (upcasted instanceof Event) {
                dispatch.accept((Event) upcasted);
            }
        }
    }

    /** Returns whether {@code from} can reach {@code to} via registered upcaster edges. */
    private boolean reachesUpcasterTarget(Class<?> from, Class<?> to) {
        Set<Class<?>> visited = new HashSet<Class<?>>();
        Deque<Class<?>> stack = new ArrayDeque<Class<?>>();
        stack.push(from);
        while (!stack.isEmpty()) {
            Class<?> current = stack.pop();
            if (current.equals(to)) {
                return true;
            }
            if (!visited.add(current)) {
                continue;
            }
            List<EventUpcaster.Typed<?, ?>> outgoing = upcasters.get(current);
            if (outgoing != null) {
                for (EventUpcaster.Typed<?, ?> u : outgoing) {
                    Class<?> next = u.getTargetType();
                    if (next != null) {
                        stack.push(next);
                    }
                }
            }
        }
        return false;
    }

    /** Snapshot of the upcaster graph for topology export. */
    Map<Class<?>, List<Class<?>>> topologySnapshot() {
        Map<Class<?>, List<Class<?>>> result = new LinkedHashMap<Class<?>, List<Class<?>>>();
        for (Map.Entry<Class<?>, List<EventUpcaster.Typed<?, ?>>> entry : upcasters.entrySet()) {
            List<Class<?>> targets = new ArrayList<Class<?>>();
            for (EventUpcaster.Typed<?, ?> u : entry.getValue()) {
                targets.add(u.getTargetType());
            }
            result.put(entry.getKey(), targets);
        }
        return result;
    }

    void clear() {
        upcasters.clear();
        hasUpcasters = false;
    }

    Map<Class<?>, List<Class<?>>> getUpcasterTopologySnapshot() {
        Map<Class<?>, List<Class<?>>> result = new java.util.LinkedHashMap<Class<?>, List<Class<?>>>();
        for (Map.Entry<Class<?>, List<EventUpcaster.Typed<?, ?>>> entry : upcasters.entrySet()) {
            List<Class<?>> targets = new ArrayList<Class<?>>();
            for (EventUpcaster.Typed<?, ?> u : entry.getValue()) {
                targets.add(u.getTargetType());
            }
            result.put(entry.getKey(), targets);
        }
        return result;
    }
}
