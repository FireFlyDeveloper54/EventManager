# EventManager

Java 8 事件总线。运行时零依赖，编译需要 Lombok。嵌入复制 `src/main/java/dev/hotaru/event`。

```java
EventManager bus = new EventManager();

class LoginEvent implements Event {}
class SaveEvent extends CancellableEvent {}

class Handlers {
    @EventTarget(Priority.HIGH)
    void onLogin(LoginEvent e) {}

    @EventTarget(ignoreCancelled = true)
    void onSave(SaveEvent e) { e.cancel(); }
}

bus.register(new Handlers());
bus.call(new LoginEvent());

Subscription sub = bus.register(SaveEvent.class, e -> e.cancel());
sub.unsubscribe();
```

| | |
|---|---|
| 注册 | `register(obj)` / `register(Class)` 静态 / `subscribe` 返回 `Subscription` |
| 分发 | `call` 含子类型；`callExact` 仅精确类型 |
| 优先级 | 越小越先：`HIGHEST 0` · `HIGH 5` · `NORMAL 10` · `LOW 15` · `LOWEST 20` |
| 取消 | `CancellableEvent` 只打标；`ignoreCancelled` 才跳过 |
| 停止 | `StoppableEvent.stop()` 中止后续；`CancellableStoppableEvent.cancel()` 同时停止 |
| 异常 | 互不影响，走 `EventErrorHandler` |

```text
gradlew.bat test
gradlew.bat build
```
