package dev.hotaru.event;

import dev.hotaru.event.impl.Event;
import java.lang.reflect.Type;

/**
 * An event that carries reified generic type information at runtime.
 * <p>
 * This enables the event bus to dispatch parameterized events (e.g. {@code MessageEvent<String>}
 * vs {@code MessageEvent<Integer>}) only to handlers that match the concrete generic type argument,
 * overcoming standard Java generic type erasure without external dependencies.
 *
 * @param <T> the payload or parameter type
 */
public interface GenericEvent<T> extends Event {

    /**
     * Returns the reified generic type of this event instance.
     *
     * @return the runtime type of the generic parameter
     */
    Type getGenericType();
}
