package com.cubk.event.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation used to mark an event handling method or {@code EventListener} field.
 */
@Target({ElementType.METHOD, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface EventTarget {
    /**
     * If true, this handler is skipped once a cancellable event has already been cancelled.
     */
    boolean ignoreCancelled() default false;
}
