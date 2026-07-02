# EventManager

A fast, thread-safe, reflective event bus for general Java 8+ projects.

## Features

- Annotation-driven handlers with `@EventTarget` and `@EventPriority`.
- Field listeners with `@EventTarget EventListener<MyEvent>`.
- Functional listeners with `register(MyEvent.class, event -> ...)` and `Subscription`.
- Static class registration with `register(MyHandlers.class)` for static handlers and static fields.
- Priority ordering where lower values run first, with stable registration-order tie breaking.
- Hierarchy dispatch: handlers registered for a supertype or interface receive subtype events.
- Cancellable and stoppable events through `CancellableEvent`, `StoppableEvent`, `ignoreCancelled`, and `stop()`.
- Lazy dispatch with `call(Class, Supplier)`, `post(Class, Supplier)`, and `fire(Class, Supplier)`.
- Handler exceptions are isolated and routed to a pluggable `EventErrorHandler`.
- Listener objects can implement `EventSubscriber` to temporarily skip their own handlers.
- Maintenance APIs: `hasListeners`, `handlerCount`, `listenerCount`, `isRegistered`, `removeEntry`, `cleanMap`, `clear`, and `unregisterAll`.
- Fast invocation through cached `LambdaMetafactory` / `MethodHandle` invokers with reflection fallback.

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

public class SaveEvent extends CancellableEvent {
}

public class WorkflowEvent extends TypedEvent {
    public WorkflowEvent(EventType type) {
        super(type);
    }
}
```

Register annotated methods:

```java
public final class AuditHandlers {
    @EventTarget
    public void onUserCreated(UserCreatedEvent event) {
        // default priority is Priority.NORMAL
    }

    @EventTarget(ignoreCancelled = true)
    @EventPriority(Priority.HIGHEST)
    public void beforeSave(SaveEvent event) {
        event.cancel();
    }
}

AuditHandlers auditHandlers = new AuditHandlers();
events.register(auditHandlers);
events.unregister(auditHandlers);
```

Register typed field listeners:

```java
public final class CacheInvalidation {
    @EventTarget
    @EventPriority(Priority.HIGH)
    private final EventListener<UserCreatedEvent> invalidateUser =
            event -> invalidate(event.getUserId());
}

events.register(new CacheInvalidation());
```

Temporarily disable a registered listener without unregistering:

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

Register lambdas:

```java
Subscription subscription = events.register(UserCreatedEvent.class, event -> {
    // handle event
});

events.register(SaveEvent.class, Priority.LOW, true, event -> {
    // skipped when the SaveEvent is already cancelled
});

subscription.unsubscribe();
```

Dispatch events:

```java
events.call(new UserCreatedEvent("42"));
events.post(new UserCreatedEvent("43"));
events.fire(new UserCreatedEvent("44"));

events.call(UserCreatedEvent.class, () -> new UserCreatedEvent("45"));
```

Use phases:

```java
events.call(new WorkflowEvent(EventType.PRE));
events.call(new WorkflowEvent(EventType.POST));
```

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

## Maintenance

```java
events.hasListeners(UserCreatedEvent.class);
events.handlerCount(UserCreatedEvent.class);
events.listenerCount();
events.isRegistered(auditHandlers);

events.removeEntry(UserCreatedEvent.class); // remove handlers registered exactly for that event type
events.unregisterAll();                     // alias for clear()
events.clear();
```

## Error Handling

Handlers that throw do not stop dispatch. Replace the default logger-backed error handler if needed:

```java
events.setErrorHandler((event, listener, throwable) -> {
    myLogger.error("handler failed for " + event, throwable);
});
```

## Priorities

`Priority` provides common constants: `HIGHEST(0)`, `HIGH(5)`, `NORMAL(10)`, `LOW(15)`, `LOWEST(20)`.
Any integer can be used; lower values run first.
