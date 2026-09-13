package dev.hotaru.event;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * An immutable metadata container associated with event dispatching.
 * <p>
 * This allows propagating correlation IDs, trace tokens, security principals,
 * or arbitrary user attributes through the event bus dispatch chain without
 * modifying event payload classes.
 */
public final class EventContext {

    private static final EventContext EMPTY = new EventContext(Collections.<String, Object>emptyMap());
    private static final ThreadLocal<EventContext> CURRENT = new ThreadLocal<EventContext>();

    private final Map<String, Object> entries;

    private EventContext(Map<String, Object> entries) {
        this.entries = Collections.unmodifiableMap(new HashMap<String, Object>(entries));
    }

    /**
     * Returns an empty event context.
     */
    public static EventContext empty() {
        return EMPTY;
    }

    /**
     * Creates an event context with a single key-value entry.
     */
    public static EventContext of(String key, Object value) {
        Objects.requireNonNull(key, "key");
        Map<String, Object> map = new HashMap<String, Object>(2);
        map.put(key, value);
        return new EventContext(map);
    }

    /**
     * Returns the current event context active on the calling thread, or {@link #empty()} if none.
     */
    public static EventContext current() {
        EventContext ctx = CURRENT.get();
        return ctx != null ? ctx : EMPTY;
    }

    /**
     * Returns a new EventContext containing all current entries plus the specified key-value pair.
     */
    public EventContext with(String key, Object value) {
        Objects.requireNonNull(key, "key");
        Map<String, Object> copy = new HashMap<String, Object>(this.entries);
        copy.put(key, value);
        return new EventContext(copy);
    }

    /**
     * Returns a new EventContext containing all current entries plus all entries from the map.
     */
    public EventContext withAll(Map<String, ?> newEntries) {
        if (newEntries == null || newEntries.isEmpty()) {
            return this;
        }
        Map<String, Object> copy = new HashMap<String, Object>(this.entries);
        copy.putAll(newEntries);
        return new EventContext(copy);
    }

    /**
     * Retrieves an attribute value by key.
     */
    public Object get(String key) {
        return entries.get(key);
    }

    /**
     * Retrieves an attribute value typed to the requested class.
     */
    public <T> T get(String key, Class<T> type) {
        Objects.requireNonNull(type, "type");
        Object val = entries.get(key);
        return type.isInstance(val) ? type.cast(val) : null;
    }

    /**
     * Retrieves an attribute value or returns the provided default value if missing or incompatible.
     */
    public <T> T getOrDefault(String key, Class<T> type, T defaultValue) {
        T val = get(key, type);
        return val != null ? val : defaultValue;
    }

    /**
     * Returns whether this context contains the specified key.
     */
    public boolean contains(String key) {
        return entries.containsKey(key);
    }

    /**
     * Returns an unmodifiable map of all entries in this context.
     */
    public Map<String, Object> asMap() {
        return entries;
    }

    /**
     * Binds this context to the calling thread for the duration of a try-with-resources block.
     *
     * @return an AutoCloseable scope that restores the previous context upon exit
     */
    public Scope attach() {
        EventContext previous = CURRENT.get();
        CURRENT.set(this);
        return new Scope(previous);
    }

    /**
     * Scoped token that restores the prior thread-local context when closed.
     */
    public static final class Scope implements AutoCloseable {
        private final EventContext previous;
        private boolean closed = false;

        private Scope(EventContext previous) {
            this.previous = previous;
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                if (previous != null) {
                    CURRENT.set(previous);
                } else {
                    CURRENT.remove();
                }
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EventContext)) return false;
        EventContext that = (EventContext) o;
        return Objects.equals(entries, that.entries);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(entries);
    }

    @Override
    public String toString() {
        return "EventContext" + entries;
    }
}
