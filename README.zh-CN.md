# EventManager

[English](README.md)

一个面向通用 Java 8+ 项目的快速、线程安全、基于反射的事件总线。

## 功能特性

- 通过注解声明处理器，可直接使用 `@EventTarget(Priority.HIGH)` 设置优先级，同时兼容
  `@EventPriority` 覆盖。
- 支持直接注册和字段形式的 `EventListener<MyEvent>` 监听器，并允许监听器定义默认优先级。
- 支持以一个分组 `Subscription` 类型安全地注册多个事件监听器。
- 支持通过 `register(MyEvent.class, event -> ...)` 和 `Subscription` 注册函数式监听器。
- 提供有状态、幂等的订阅，支持 `isSubscribed()`、`and(...)` 和
  `Subscriptions.combine(...)`。
- 支持通过 `register(MyHandlers.class)` 注册静态处理器和静态字段。
- 按优先级排序，数值越小越先执行；优先级相同时，按注册顺序稳定排序。
- 默认按类型层次结构分发，也可通过 `callExact(...)` 仅按精确类型分发。
- 通过 `CancellableEvent`、`StoppableEvent`、`ignoreCancelled` 和 `stop()` 支持事件取消与传播停止。
- 支持通过 Supplier 重载延迟创建并分发层次结构事件或精确类型事件。
- 支持通过 `call(event, afterDispatch)` 注册仅执行一次的分发完成回调，`post` 和 `fire`
  同样提供该功能。
- 处理器异常彼此隔离，并交给可替换的 `EventErrorHandler` 处理。
- 监听器对象可以实现 `EventSubscriber`，以临时跳过自身的处理器。
- 提供用于层次结构注册和精确类型注册的维护 API，包括监听器计数、移除与清理。
- 缓存监听器扫描计划，以及基于 `LambdaMetafactory` / `MethodHandle` 的调用器，并在必要时回退到反射调用。

## 基本用法

```java
EventManager events = new EventManager();
```

定义事件：

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

## 注解处理器

```java
public final class AuditHandlers {
    @EventTarget(Priority.HIGH)
    public void onUserCreated(UserCreatedEvent event) {
        // 直接声明优先级。
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

对于方法，`@EventTarget` 的默认优先级是 `Priority.NORMAL`。为了保持源码兼容性，仍然支持单独使用
`@EventPriority`；当两个注解都指定了优先级时，以 `@EventPriority` 为准。

## 类型化监听器

注册一个类型化字段监听器：

```java
public final class CacheInvalidation {
    @EventTarget
    @EventPriority(Priority.HIGH)
    private final EventListener<UserCreatedEvent> invalidateUser =
            event -> invalidate(event.getUserId());
}

events.register(new CacheInvalidation());
```

如果没有 `@EventPriority` 注解，字段监听器可以提供自己的优先级：

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

`@EventTarget(Priority.HIGH)` 或 `@EventPriority(Priority.HIGH)` 会覆盖字段监听器的
`getPriority()`。当两者同时存在时，以 `@EventPriority` 为准。

不扫描字段，直接注册类型化监听器：

```java
Subscription metrics = events.registerListener(
        UserCreatedEvent.class,
        Priority.LOW,
        event -> record(event.getUserId())
);
```

为多个相关事件类注册同一个监听器：

```java
EventListener<Event> lifecycle = event -> recordLifecycle(event);

Subscription lifecycleSubscription = events.registerListener(
        lifecycle,
        UserCreatedEvent.class,
        SaveEvent.class,
        WorkflowEvent.class
);
```

重复项和 `null` 类会被忽略。取消返回的分组订阅会移除其中的全部注册项。

## 函数式监听器与订阅

```java
Subscription audit = events.register(UserCreatedEvent.class, event -> {
    // 处理事件。
});

Subscription cancelledAware = events.register(
        SaveEvent.class,
        Priority.LOW,
        true,
        event -> {
            // 当 SaveEvent 已被取消时跳过。
        }
);

Subscription group = audit.and(cancelledAware);
group.isSubscribed(); // 只要至少有一个子注册仍有效，就返回 true
group.unsubscribe();  // 以幂等方式移除两个注册项
```

`Subscriptions.combine(first, second, third)` 可以组合任意数量的注册项。

## 事件分发

普通分发会包含为兼容父类和接口注册的处理器：

```java
events.call(new UserCreatedEvent("42"));
events.post(new UserCreatedEvent("43"));
events.fire(new UserCreatedEvent("44"));

events.call(new AdminCreatedEvent("45"));
// AdminCreatedEvent 和 UserCreatedEvent 的处理器都可以执行。
```

如果只应执行为事件运行时类型注册的处理器，请使用精确类型分发：

```java
events.callExact(new AdminCreatedEvent("46"));

events.callExact(
        AdminCreatedEvent.class,
        () -> new AdminCreatedEvent("47")
);
```

没有匹配的处理器时，不会调用 Supplier。`call(Class, Supplier)` 检查层次结构监听器，
而 `callExact(Class, Supplier)` 只检查精确类型注册项。

在分发完成后执行一次回调，即使该事件没有处理器也会执行：

```java
events.call(new UserCreatedEvent("48"), () -> {
    flushAuditBatch();
});
```

`post(event, afterDispatch)` 和 `fire(event, afterDispatch)` 是对应的别名。

## 取消与停止传播

取消是由应用定义的结果标记。它通常用于通知事件发布方，不再执行与该事件关联的默认操作。
`EventManager` 本身不会执行该操作，也不会解释取消在业务上的含义。

```java
events.register(SaveEvent.class, Priority.HIGHEST, event -> event.cancel());

events.register(SaveEvent.class, Priority.NORMAL, true, event -> {
    // 由于 ignoreCancelled 为 true，事件被取消后会跳过此监听器。
});

events.register(SaveEvent.class, Priority.LOW, false, event -> {
    // 即使事件已被取消，仍然会收到该事件。
});

SaveEvent event = events.call(new SaveEvent());
if (!event.isCancelled()) {
    persistChanges();
}
```

取消与停止传播被刻意设计为两个独立概念：

- `cancel()` 只改变事件的取消状态。由发布方决定应禁止哪项默认行为。
- `ignoreCancelled = true` 表示当前面的处理器已经取消事件时，跳过该处理器。
- 对 `StoppableEvent` 调用 `stop()` 会在执行下一个处理器前终止事件总线分发。

可变的决策事件也可以暴露比取消更具体的属性，例如 `setAllowed(false)`、
`setSlowdown(false)` 或替换结果。发布方在分发结束后读取该属性。

## 可启停订阅者

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

对象在禁用时仍保持注册状态，因此重新启用时不需要再次进行反射扫描或注册。

## 静态处理器

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

传入一个类时，只会注册带注解的静态成员。

## 类型事件

```java
events.call(new WorkflowEvent(EventType.PRE));
events.call(new WorkflowEvent(EventType.POST));
```

`TypedEvent` 保存阶段或类型，处理器自行决定要处理哪些阶段。

## 维护操作

```java
events.hasListeners(UserCreatedEvent.class);
events.handlerCount(UserCreatedEvent.class);

events.hasExactListeners(UserCreatedEvent.class);
events.exactHandlerCount(UserCreatedEvent.class);

events.listenerCount();
events.handlerCount();
events.isRegistered(auditHandlers);

events.removeEntry(UserCreatedEvent.class); // 移除此类型的精确注册项。
events.cleanMap(true);                      // 移除空存储桶。
events.unregisterAll();                     // clear() 的别名。
events.clear();
```

## 错误处理

某个处理器抛出异常时，不会中断事件分发。需要时可以替换默认的日志错误处理器：

```java
events.setErrorHandler((event, listener, throwable) -> {
    myLogger.error("handler failed for " + event, throwable);
});
```

即使最终回退到反射调用，传给错误处理器的仍然是监听器最初抛出的异常。

## 继承规则

系统会扫描类和接口的完整层次结构中的监听器方法与字段。生成的不可变扫描计划会按监听器类缓存，
并在注册该类的每个实例时重复使用。

即使重写方法没有添加 `@EventTarget`，它仍会遮蔽具有相同 Java 签名的继承处理器。
私有方法以及实际上不可重写的包私有方法仍会作为独立处理器存在。

## 性能

- 事件存储桶使用 `ConcurrentHashMap` 和 `CopyOnWriteArrayList`。
- 分发数组会按运行时事件类扁平化、排序并缓存。
- 监听器类的扫描计划会被缓存，避免重复进行注解与层次结构分析。
- 每个方法的调用器工厂会被缓存。
- 调用时依次优先使用 `LambdaMetafactory`、`MethodHandle`，最后回退到反射。
- 优先级相同时，注册顺序保持稳定。

## 源码嵌入

主源码树没有任何外部运行时或编译时依赖。要将该库嵌入其他项目，请把下面的完整目录复制到
目标项目的 Java 源码根目录：

```text
src/main/java/com/cubk/event
```

请保持目录结构和包声明不变。例如，复制到 Gradle 项目后，目标路径仍应为：

```text
other-project/src/main/java/com/cubk/event/EventManager.java
other-project/src/main/java/com/cubk/event/annotations/EventTarget.java
other-project/src/main/java/com/cubk/event/impl/Event.java
```

这样就不需要在 Gradle 或 Maven 中添加 EventManager 依赖。复制的源码只使用 JDK 8 API 和
Java 8 语法，因此同一份源码树可以在 JDK 8 至 JDK 25 上编译和运行。

目标项目自身的编译器可以是 JDK 8。Gradle 9 仅用于让本仓库的验证构建能够在本地 JDK 25
环境下启动，并不是嵌入源码的依赖。

验证任务 `compileSourceEmbeddingExample` 会使用空 classpath，将
`examples/source-embedding/EmbeddedUsage.java` 与原始主源码树一起编译，以发现意外引入的
项目专用依赖。

## 优先级

`Priority` 提供以下常用常量：

- `FIRST(Integer.MIN_VALUE + 1)`
- `HIGHEST(0)`
- `HIGH(5)`
- `NORMAL(10)`
- `MEDIUM(10)`，是 `NORMAL` 的别名
- `LOW(15)`
- `LOWEST(20)`
- `LAST(Integer.MAX_VALUE - 1)`
- `MONITOR(Integer.MAX_VALUE)`

数值越小越先执行。`MONITOR` 在 `LAST` 之后执行，适合用于最终观察。
`Priority.UNSPECIFIED` 被保留为 `@EventTarget` 的继承哨兵值，不得用于注册监听器。

## 构建与测试

```text
gradlew.bat test
gradlew.bat build
gradlew.bat test -PtestJavaVersion=25
gradlew.bat runSourceEmbeddingExample -PtestJavaVersion=25
```

编译使用 JDK 8 工具链。测试和源码嵌入示例默认在 JDK 8 上运行；
`-PtestJavaVersion=25` 用于验证相同的 Java 8 字节码可以在 JDK 25 上运行。

测试套件覆盖层次结构与精确类型分发、直接注解优先级、最先/最后/监视顺序、字段与静态处理器、
多事件注册、订阅组合、取消、停止传播、继承重写、多实例注册缓存、完成回调、延迟分发、
异常隔离以及零依赖源码嵌入。
