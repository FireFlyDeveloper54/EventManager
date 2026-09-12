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
}
