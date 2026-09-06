# EventManager

一个无运行时依赖、基于注解的通用 Java 事件总线，兼容 JDK 8 至 JDK 25。
Lombok 仅用于编译期生成样板代码，运行时不需要 Lombok。

## 基本用法

```java
EventManager bus = new EventManager();

final class LoginEvent implements Event {}
final class SaveEvent extends CancellableEvent {}

final class Handlers {
    @EventTarget(Priority.HIGH)
    public void onLogin(LoginEvent event) {}

    @EventTarget(ignoreCancelled = true)
    public void onSave(SaveEvent event) {}
}

bus.register(new Handlers());
bus.call(new LoginEvent());

Subscription subscription = bus.register(SaveEvent.class, SaveEvent::cancel);
subscription.unsubscribe();

Subscription once = bus.registerOnce(LoginEvent.class, event -> logIn(event));

Subscription filtered = bus.registerFiltered(LoginEvent.class,
        event -> event.isValid(), event -> logIn(event));

CompletableFuture<LoginEvent> done = bus.callAsync(new LoginEvent(), executor);
```

## 行为语义

- `call(event)` 会分发给事件运行时类型及其父类、接口对应的监听器。
- `callExact(event)` 只分发给与运行时类型完全相同的监听器。
- `callAsync(event, executor)` 和 `callExactAsync(event, executor)` 使用调用方提供的
  `Executor`，事件总线不会自行创建或持有线程池。
- 优先级数值越小越先执行（`HIGHEST`、`HIGH`、`NORMAL`、`LOW`、`LOWEST`）。
- 可取消事件取消后，`ignoreCancelled = true` 的监听器会被跳过。
- 可停止事件在当前监听器完成后停止后续分发。
- 监听器异常会交给 `EventErrorHandler`：默认 `ErrorPolicy.CONTINUE` 会报告后继续；
  `STOP` 会停止本次分发；`PROPAGATE` 会把异常传播给调用者。
- 分发使用不可变快照。分发过程中新增的监听器从下一次事件开始生效。
- 分发过程中注销的监听器会被跳过，不会因为快照已经生成而再次执行。
- `subscribe(...)` 只拥有本次订阅创建的监听器；对已注册监听器重复订阅会返回
  `Subscription.NOOP`。
- `registerOnce(...)` / `registerListenerOnce(...)` 创建一次性监听器，触发前自动注销；
  即使回调中重入分发，也只会执行一次。
- `registerFiltered(...)` 可在进入监听器前执行类型安全的事件过滤器；过滤器返回 `false` 时不会调用回调。
- 注解监听字段在分发时重新读取，替换可变字段中的回调后无需重新注册。
- 参数化父类或接口中的泛型 `EventListener<T>` 字段会在扫描监听器类型时解析。
- 反射缓存使用 `ClassValue`，有利于可卸载的类加载器及时回收。
- `metrics()` 可获取全局分发、实际调用和失败次数的不可变快照；`metrics(Event.class)` 可按运行时事件类型查看；
  `resetMetrics()` 只重置统计，不影响注册。
- `EventManager` 实现 `AutoCloseable`，`close()` 会清理所有监听器和缓存，可安全重复调用。
- `registeredEventTypes()` 返回已注册事件类型的只读快照，`isEmpty()` 可用于健康检查或停机判断。

## 构建与测试

```text
gradlew.bat clean build -PtestJavaVersion=8
gradlew.bat test -PtestJavaVersion=25
```

公开 API 不包含 Minecraft、Fabric、Forge 或其他游戏特化类型。
