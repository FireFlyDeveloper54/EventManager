package dev.hotaru.event.impl;

import dev.hotaru.event.GenericEvent;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * Convenient base implementation for {@link GenericEvent} that automatically captures its
 * generic type argument from its generic superclass at instantiation time, or accepts an
 * explicit {@link Type}.
 *
 * @param <T> the generic parameter type
 */
public abstract class AbstractGenericEvent<T> implements GenericEvent<T> {

    private final Type genericType;

    /**
     * Automatically extracts the generic type argument from the subclass declaration.
     */
    protected AbstractGenericEvent() {
        Type superClass = getClass().getGenericSuperclass();
        if (superClass instanceof ParameterizedType) {
            this.genericType = ((ParameterizedType) superClass).getActualTypeArguments()[0];
        } else {
            this.genericType = Object.class;
        }
    }

    /**
     * Creates a generic event with an explicitly specified generic type.
     *
     * @param genericType the runtime type token, or {@code null} to default to {@code Object.class}
     */
    protected AbstractGenericEvent(Type genericType) {
        this.genericType = genericType != null ? genericType : Object.class;
    }

    @Override
    public Type getGenericType() {
        return genericType;
    }
}
