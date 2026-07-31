package com.cubk.event.annotations;

import com.cubk.event.Priority;

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
     * Priority used by this handler. {@link Priority#UNSPECIFIED} keeps the default behaviour:
     * methods use {@link Priority#NORMAL}, while field listeners use their own priority.
     *
     * @return declared priority or {@link Priority#UNSPECIFIED}
     */
    int value() default Priority.UNSPECIFIED;

    /**
     * If true, this handler is skipped once a cancellable event has already been cancelled.
     *
     * @return whether cancelled events should be ignored
     */
    boolean ignoreCancelled() default false;
}
