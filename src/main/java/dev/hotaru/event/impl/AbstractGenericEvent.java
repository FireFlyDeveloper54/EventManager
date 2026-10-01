package dev.hotaru.event.impl;

import java.lang.reflect.Type;

/**
 * @deprecated Moved to {@link dev.hotaru.event.AbstractGenericEvent}. This
 * class now extends it and remains only for source and binary compatibility.
 *
 * @param <T> the generic parameter type
 */
@Deprecated
public abstract class AbstractGenericEvent<T> extends dev.hotaru.event.AbstractGenericEvent<T> {

    protected AbstractGenericEvent() {
        super();
    }

    protected AbstractGenericEvent(Type genericType) {
        super(genericType);
    }
}
