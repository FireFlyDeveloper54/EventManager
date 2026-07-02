package com.cubk.event.annotations;

import java.lang.annotation.*;

/**
 * Annotation used to mark a method as an event handling method.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface EventTarget {
    /**
     * If true, this handler is skipped once a cancellable event has already been cancelled.
     */
    boolean ignoreCancelled() default false;
}
