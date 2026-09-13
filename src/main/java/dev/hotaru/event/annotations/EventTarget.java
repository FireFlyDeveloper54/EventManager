package dev.hotaru.event.annotations;

import dev.hotaru.event.EventFilter;
import dev.hotaru.event.Priority;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.METHOD, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface EventTarget {

    int value() default Priority.UNSPECIFIED;

    boolean ignoreCancelled() default false;

    Class<? extends EventFilter> filter() default EventFilter.PassAll.class;

    boolean sticky() default false;

    /**
     * Unique identifier for this handler in topological DAG ordering.
     */
    String id() default "";

    /**
     * Handler IDs that must execute before this handler.
     */
    String[] after() default {};

    /**
     * Handler IDs that must execute after this handler.
     */
    String[] before() default {};

    /**
     * Listener classes that must execute before this handler.
     */
    Class<?>[] afterClasses() default {};

    /**
     * Listener classes that must execute after this handler.
     */
    Class<?>[] beforeClasses() default {};
}
