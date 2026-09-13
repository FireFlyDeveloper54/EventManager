package dev.hotaru.event;

import java.util.Objects;
import java.util.function.Function;

/**
 * Transforms an older or alternate representation of an event into an updated or target representation.
 *
 * @param <S> the source event type
 * @param <T> the target event type
 */
@FunctionalInterface
public interface EventUpcaster<S, T> {

    /**
     * Upcasts the given source event into the target event representation.
     *
     * @param source the source event, never null
     * @return the upcasted target event, never null
     */
    T upcast(S source);

    /**
     * Creates an upcaster from source and target classes with a mapping function.
     *
     * @param sourceType the source event class
     * @param targetType the target event class
     * @param mapper the mapping function
     * @param <S> source type
     * @param <T> target type
     * @return a typed EventUpcaster
     */
    static <S, T> Typed<S, T> of(Class<S> sourceType, Class<T> targetType, Function<S, T> mapper) {
        Objects.requireNonNull(sourceType, "sourceType");
        Objects.requireNonNull(targetType, "targetType");
        Objects.requireNonNull(mapper, "mapper");
        return new Typed<S, T>() {
            @Override
            public Class<S> getSourceType() {
                return sourceType;
            }

            @Override
            public Class<T> getTargetType() {
                return targetType;
            }

            @Override
            public T upcast(S source) {
                return mapper.apply(source);
            }
        };
    }

    /**
     * A typed upcaster that explicitly exposes source and target types.
     *
     * @param <S> source type
     * @param <T> target type
     */
    interface Typed<S, T> extends EventUpcaster<S, T> {
        Class<S> getSourceType();
        Class<T> getTargetType();
    }
}
