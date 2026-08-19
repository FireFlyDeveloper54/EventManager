# EventManager

[简体中文](README.zh-CN.md)

A fast, thread-safe, reflective event bus for general Java 8+ projects.

## Features

- Annotation-driven handlers with direct `@EventTarget(Priority.HIGH)` priorities and compatible
  `@EventPriority` overrides.
- Direct and field-based `EventListener<MyEvent>` listeners with listener-defined default priorities.
- Type-safe multi-event listener registration with one grouped `Subscription`.
- Functional listeners with `register(MyEvent.class, event -> ...)` and `Subscription`.
- Stateful, idempotent subscriptions with `isSubscribed()`, `and(...)`, and `Subscriptions.combine(...)`.
- Static class registration with `register(MyHandlers.class)` for static handlers and static fields.
- Priority ordering where lower values run first, with stable registration-order tie breaking.
- Hierarchy dispatch by default, plus exact-class dispatch with `callExact(...)`.
- Cancellable and stoppable events through `CancellableEvent`, `StoppableEvent`, `ignoreCancelled`, and `stop()`.
- Lazy hierarchy and exact dispatch with supplier overloads.
- One-shot completion callbacks with `call(event, afterDispatch)`, also available through `post` and `fire`.
- Handler exceptions are isolated and routed to a pluggable `EventErrorHandler`.
- Listener objects can implement `EventSubscriber` to temporarily skip their own handlers.
- Maintenance APIs for hierarchy and exact registrations, listener counts, removal, and cleanup.
- Cached listener scan plans and cached `LambdaMetafactory` / `MethodHandle` invokers with reflection fallback.

## Basic Usage

```java
EventManager events = new EventManager();
```

Define events:

```java
public class UserCreatedEvent implements Event {
    private final String userId;

    public UserCreatedEvent(String userId) {
        this.userId = userId;
    }

    public String getUserId() {
        return userId;
    }
}

public class AdminCreatedEvent extends UserCreatedEvent {
    public AdminCreatedEvent(String userId) {
        super(userId);
    }
}

public class SaveEvent extends CancellableEvent {
}

public class WorkflowEvent extends TypedEvent {
    public WorkflowEvent(EventType type) {
        super(type);
    }
}
```

## Annotated Handlers

```java
public final class AuditHandlers {
    @EventTarget(Priority.HIGH)
    public void onUserCreated(UserCreatedEvent event) {
        // Direct priority declaration.
    }

    @EventTarget(value = Priority.HIGH, ignoreCancelled = true)
    @EventPriority(Priority.HIGHEST)
    public void beforeSave(SaveEvent event) {
        event.cancel();
    }
}

AuditHandlers auditHandlers = new AuditHandlers();
events.register(auditHandlers);
events.unregister(auditHandlers);
```

`@EventTarget` defaults to `Priority.NORMAL` for methods. A separate `@EventPriority` remains
supported for source compatibility and takes precedence when both annotations specify a priority.

## Typed Listeners

Register a typed field listener:

```java
public final class CacheInvalidation {
    @EventTarget
    @EventPriority(Priority.HIGH)
    private final EventListener<UserCreatedEvent> invalidateUser =
            event -> invalidate(event.getUserId());
}

events.register(new CacheInvalidation());
```

Without an `@EventPriority` annotation, a field listener can provide its own priority:

```java
public final class MetricsHandlers {
    @EventTarget
    private final EventListener<UserCreatedEvent> metrics =
            new EventListener<UserCreatedEvent>() {
                @Override
                public void onEvent(UserCreatedEvent event) {
                    record(event.getUserId());
                }

                @Override
                public int getPriority() {
                    return Priority.LOW;
                }
            };
}
```

`@EventTarget(Priority.HIGH)` or `@EventPriority(Priority.HIGH)` overrides a field listener's
`getPriority()`. When both are present, `@EventPriority` wins.

Register a typed listener directly without reflective field scanning:

```java
Subscription metrics = events.registerListener(
        UserCreatedEvent.class,
        Priority.LOW,
        event -> record(event.getUserId())
);
```

Register one listener for several related event classes:

```java
EventListener<Event> lifecycle = event -> recordLifecycle(event);

Subscription lifecycleSubscription = events.registerListener(
        lifecycle,
        UserCreatedEvent.class,
        SaveEvent.class,
        WorkflowEvent.class
);
```

Duplicate and `null` classes are ignored. Unsubscribing the returned grouped subscription removes
every registration.

## Functional Listeners And Subscriptions

```java
Subscription audit = events.register(UserCreatedEvent.class, event -> {
    // Handle the event.
});

Subscription cancelledAware = events.register(
        SaveEvent.class,
        Priority.LOW,
        true,
        event -> {
            // Skipped when the SaveEvent is already cancelled.
        }
);

Subscription group = audit.and(cancelledAware);
group.isSubscribed(); // true while at least one child registration is active
group.unsubscribe();  // idempotently removes both registrations
```

`Subscriptions.combine(first, second, third)` can combine any number of registrations.

## Dispatch

Normal dispatch includes handlers registered for compatible superclasses and interfaces:

```java
events.call(new UserCreatedEvent("42"));
events.post(new UserCreatedEvent("43"));
events.fire(new UserCreatedEvent("44"));

events.call(new AdminCreatedEvent("45"));
// Both AdminCreatedEvent and UserCreatedEvent handlers can run.
```

Use exact dispatch when only handlers registered for the runtime class should run:

```java
events.callExact(new AdminCreatedEvent("46"));

events.callExact(
        AdminCreatedEvent.class,
        () -> new AdminCreatedEvent("47")
);
```

The supplier is not invoked when no matching handler exists. `call(Class, Supplier)` checks
hierarchy listeners, while `callExact(Class, Supplier)` checks exact registrations only.

Run a callback once after dispatch completes, including when the event has no handlers:

```java
events.call(new UserCreatedEvent("48"), () -> {
    flushAuditBatch();
});
```

`post(event, afterDispatch)` and `fire(event, afterDispatch)` are aliases.

## Cancellation And Stopping

Cancellation is an application-defined result flag. It commonly tells the code that posted the
event to suppress its associated default action. `EventManager` does not perform that action or
interpret the business meaning of cancellation.

```java
events.register(SaveEvent.class, Priority.HIGHEST, event -> event.cancel());

events.register(SaveEvent.class, Priority.NORMAL, true, event -> {
    // Skipped after cancellation because ignoreCancelled is true.
});

events.register(SaveEvent.class, Priority.LOW, false, event -> {
    // Still receives a cancelled event.
});

SaveEvent event = events.call(new SaveEvent());
if (!event.isCancelled()) {
    persistChanges();
}
```

Cancellation and propagation stopping are deliberately separate:

- `cancel()` only changes the event's cancellation state. The publisher decides what default
  behaviour that suppresses.
- `ignoreCancelled = true` skips a handler when an earlier handler has cancelled the event.
- `stop()` on a `StoppableEvent` ends event-bus dispatch before the next handler.

Mutable decision events can also expose a more specific property instead of cancellation, such as
`setAllowed(false)`, `setSlowdown(false)`, or a replacement result. The publisher then reads that
property after dispatch.

## Toggleable Subscribers

```java
public final class ToggleableHandlers implements EventSubscriber {
    private boolean enabled;

    @EventTarget
    public void onUserCreated(UserCreatedEvent event) {
    }

    @Override
    public boolean isHandlingEvents() {
        return enabled;
    }
}
```

The object remains registered while disabled, so enabling it does not require another reflection
scan or registration.

## Static Handlers

```java
public final class GlobalHandlers {
    @EventTarget
    public static void onUserCreated(UserCreatedEvent event) {
    }

    @EventTarget
    private static final EventListener<SaveEvent> saveListener = event -> {
    };
}

events.register(GlobalHandlers.class);
events.unregister(GlobalHandlers.class);
```

Passing a class registers static annotated members only.

## Typed Events

```java
events.call(new WorkflowEvent(EventType.PRE));
events.call(new WorkflowEvent(EventType.POST));
```

`TypedEvent` stores the phase/type. Handlers decide which phases they process.

## Maintenance

```java
events.hasListeners(UserCreatedEvent.class);
events.handlerCount(UserCreatedEvent.class);

events.hasExactListeners(UserCreatedEvent.class);
events.exactHandlerCount(UserCreatedEvent.class);

events.listenerCount();
events.handlerCount();
events.isRegistered(auditHandlers);

events.removeEntry(UserCreatedEvent.class); // Remove exact registrations for this type.
events.cleanMap(true);                      // Remove empty buckets.
events.unregisterAll();                     // Alias for clear().
events.clear();
```

## Error Handling

Handlers that throw do not stop dispatch. Replace the default logger-backed error handler if needed:

```java
events.setErrorHandler((event, listener, throwable) -> {
    myLogger.error("handler failed for " + event, throwable);
});
```

The original listener exception is passed to the error handler even when reflection is used as the
final invocation fallback.

## Inheritance Rules

Listener methods and fields are scanned across the complete class and interface hierarchy. The
resulting immutable scan plan is cached per listener class and reused for every registered instance.

An overriding method shadows an inherited handler with the same Java signature, even when the
override omits `@EventTarget`. Private methods and package-private methods that are not actually
overridable remain independent handlers.

## Performance

- Event buckets use `ConcurrentHashMap` and `CopyOnWriteArrayList`.
- Dispatch arrays are flattened, sorted, and cached per runtime event class.
- Listener class scan plans are cached, avoiding repeated annotation and hierarchy analysis.
- Per-method invoker factories are cached.
- Invocation prefers `LambdaMetafactory`, then `MethodHandle`, then reflection.
- Registration order is stable when priorities are equal.

## Source Embedding

The main source tree has no external runtime or compile-time dependencies. To embed the library,
copy this complete directory into another project's Java source root:

```text
src/main/java/com/cubk/event
```

Keep the directory structure and package declarations intact. For example, after copying into a
Gradle project, the destination remains:

```text
other-project/src/main/java/com/cubk/event/EventManager.java
other-project/src/main/java/com/cubk/event/annotations/EventTarget.java
other-project/src/main/java/com/cubk/event/impl/Event.java
```

No EventManager dependency entry is then required in Gradle or Maven. The copied sources use only
JDK 8 APIs and Java 8 language syntax, so the same source tree can be compiled for and run on JDK 8
through JDK 25.

The destination project's own compiler can be JDK 8. Gradle 9 is used only by this repository's
verification build so it can launch under the local JDK 25; it is not a dependency of the embedded
sources.

`examples/source-embedding/EmbeddedUsage.java` is compiled together with the raw main source tree,
using an empty classpath, by the `compileSourceEmbeddingExample` verification task. This catches
accidental dependencies on project-specific libraries.

## Priorities

`Priority` provides common constants:

- `FIRST(Integer.MIN_VALUE + 1)`
- `HIGHEST(0)`
- `HIGH(5)`
- `NORMAL(10)`
- `MEDIUM(10)`, an alias for `NORMAL`
- `LOW(15)`
- `LOWEST(20)`
- `LAST(Integer.MAX_VALUE - 1)`
- `MONITOR(Integer.MAX_VALUE)`

Lower values run first. `MONITOR` runs after `LAST`, making it suitable for final observation.
`Priority.UNSPECIFIED` is reserved as the `@EventTarget` inheritance sentinel and must not be used
for listener registration.

## Build And Test

```text
gradlew.bat test
gradlew.bat build
gradlew.bat test -PtestJavaVersion=25
gradlew.bat runSourceEmbeddingExample -PtestJavaVersion=25
```

Compilation uses a JDK 8 toolchain. Tests and the source-embedding example run on JDK 8 by default;
`-PtestJavaVersion=25` verifies the same Java 8 bytecode on JDK 25.

The test suite covers hierarchy and exact dispatch, direct annotation priorities, first/last/monitor
ordering, field and static handlers, multi-event registration, subscription composition,
cancellation, stopping, inheritance overrides, cached multi-instance registration, completion
callbacks, lazy dispatch, exception isolation, and zero-dependency source embedding.
