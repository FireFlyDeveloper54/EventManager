# EventManager

一个无任何第三方依赖（100% 纯 JDK 原生标准库实现，零编译期/运行时外部依赖）、基于注解与 Lambda 的高性能通用 Java 事件总线，全面兼容 JDK 8 至 JDK 25。

## 核心特性

- **极致性能**：基于 `ClassValue` 与 `LambdaMetafactory` 实现进程级动态编译与元数据复用；分发使用不可变快照，单类型事件零集合分配。
- **无锁并发**：处理器增删全流程采用 CAS 无锁循环，杜绝全局粗粒度锁竞争与 `ConcurrentModificationException`。
- **内存安全**：原生支持弱引用托管（`registerWeak` / `subscribeWeak`），彻底消除长生命周期事件总线导致的 *Lapsed Listener* 内存泄漏。
- **死信检测**：原生支持 `DeadEvent` 未处理事件感知，防止复杂系统与插件架构中事件因时序颠倒而静默丢失。
- **声明式过滤**：支持 `@EventTarget(filter = MyFilter.class)`，以声明方式将事件过滤器绑定至监听器，无需手动在回调中书写 `if` 判断。
- **拦截器管道**：提供 `EventInterceptor` 与 `EventInterceptors.chain` 管道组合器，轻松实现全链路追踪、性能度量与安全短路。
- **线程亲和性**：支持配置 `enforceThread(...)`，在非法线程派发时即时抛出明确诊断异常，彻底杜绝游戏/UI 主线程竞态。
- **分层作用域**：支持 `createChildBus()` 创建子总线，事件就地处理后向上冒泡，子模块关闭时一键解绑并释放全部资源。
- **可观察性**：内置 APM 延迟分析器，纳秒级记录各事件类型的累计总耗时、峰值耗时（Max Spike）与平均延迟。

## 基本用法

```java
// 1. 使用流式建造者构造总线
EventManager bus = EventManager.builder()
        .errorPolicy(ErrorPolicy.CONTINUE)
        .metricsEnabled(true)
        .deadEventsEnabled(true)
        .enforceThread(Thread.currentThread()) // 绑定主线程派发
        .build();

final class LoginEvent implements Event {
    public boolean isAdmin;
}
final class SaveEvent extends CancellableEvent {}

// 2. 自定义声明式过滤器
final class AdminFilter implements EventFilter<LoginEvent> {
    @Override
    public boolean test(LoginEvent event) {
        return event.isAdmin;
    }
}

final class Handlers {
    // 仅当 event.isAdmin == true 时进入该方法
    @EventTarget(value = Priority.HIGH, filter = AdminFilter.class)
    public void onAdminLogin(LoginEvent event) {}

    @EventTarget(ignoreCancelled = true)
    public void onSave(SaveEvent event) {}
}

// 3. 基础注册与分发
bus.register(new Handlers());
bus.call(new LoginEvent());

// 4. 批量分发（支持变长参数与 Iterable 集合）
bus.callAll(new LoginEvent(), new SaveEvent());

// 5. 可取消事件便捷判定
boolean cancelled = bus.callCancelled(new SaveEvent());

// 6. 弱引用监听器（目标对象被 GC 后，处理器自动标记失效并跳过）
bus.registerWeak(new Handlers());
Subscription weakSub = bus.subscribeWeak(SaveEvent.class, event -> {});

// 7. 死信事件捕获（当分发的事件没有任何有效监听器时触发）
bus.register(DeadEvent.class, dead -> {
    System.out.println("未处理事件: " + dead.getEvent() + ", 来源: " + dead.getSource());
});

// 8. 拦截器链式追加（AOP / APM / 安全审计）
bus.addInterceptor((event, proceed) -> {
    long start = System.nanoTime();
    proceed.run(); // 继续分发；若不调用则短路拦截
    long cost = System.nanoTime() - start;
});

// 9. 分层/子作用域总线（插件与临时场景生命周期隔离）
EventManager pluginBus = bus.createChildBus();
pluginBus.register(SaveEvent.class, event -> {
    // 优先在子总线执行，随后自动冒泡至父总线
});
pluginBus.close(); // 插件卸载：一键解绑并清理子级全部监听器

// 10. APM 性能度量读取
EventMetrics globalMetrics = bus.metrics();
System.out.printf("总分发数: %d, 总耗时: %d ns, 最大峰值: %d ns, 平均耗时: %.2f ns%n",
        globalMetrics.getDispatchedEvents(),
        globalMetrics.getTotalDurationNanos(),
        globalMetrics.getMaxDurationNanos(),
        globalMetrics.getAverageDurationNanos());

// 11. 条件批量注销与一键清理
bus.unregisterIf(listener -> listener instanceof Handlers);
bus.unregisterEventType(SaveEvent.class);
bus.unregisterAll();
```

## 行为语义

- `call(event)` 会分发给事件运行时类型及其父类、接口对应的监听器。
- `callExact(event)` 只分发给与运行时类型完全相同的监听器。
- `callAll(Event...)` / `callAll(Iterable)` 按顺序批量流转分发事件。
- `callCancelled(event)` 分发可取消事件并直接返回最终是否被取消（`event.isCancelled()`）。
- `callAsync(event, executor)` 和 `callExactAsync(event, executor)` 使用调用方提供的 `Executor`，事件总线不强行持有线程池。
- 优先级数值越小越先执行（`HIGHEST`、`HIGH`、`NORMAL`、`LOW`、`LOWEST`）。
- 可取消事件取消后，`ignoreCancelled = true` 的监听器会被跳过。
- 可停止事件在当前监听器完成后停止后续分发。
- 监听器异常会交给 `EventErrorHandler`：默认 `ErrorPolicy.CONTINUE` 会报告后继续；`STOP` 会停止本次分发；`PROPAGATE` 会把异常传播给调用者。
- 分发使用不可变快照。分发过程中新增的监听器从下一次事件开始生效；注销的监听器会被即时标记跳过。
- `subscribe(...)` 只拥有本次订阅创建的监听器；对已注册监听器重复订阅会返回 `Subscription.NOOP`。
- `registerOnce(...)` / `registerListenerOnce(...)` 创建一次性监听器，触发前自动注销；即使回调中重入分发，也只会执行一次。
- `registerFiltered(...)` / `@EventTarget(filter = ...)` 在进入监听器前执行类型安全的过滤器；过滤器返回 `false` 时跳过。
- `registerWeak(...)` / `subscribeWeak(...)` 采用弱引用托管监听器目标，对象被垃圾回收后处理器自失效并可在后续分发或注销中安全跳过，根除经典 *Lapsed Listener* 内存泄漏。
- `unregisterIf(Predicate<Object>)` 支持按自定义谓词条件批量注销匹配的监听器。
- `unregisterEventType(Class<? extends Event>)` 支持清理特定事件类型的所有直接处理器。
- `addInterceptor(EventInterceptor)` / `EventInterceptors.chain` 支持管道化分发拦截，可用于耗时统计、全链路跟踪、AOP 切面或有条件短路；未设置时 0 额外开销。
- `DeadEvent`：当事件无任何监听器接收且总线开启死信捕获时，自动封装并派发，杜绝孤儿事件静默丢失；自身派发不会造成无限递归。
- `enforceThread(...)`：在派发前校验当前线程是否合法，非法则立即抛出 `IllegalStateException`。
- `createChildBus()`：创建子作用域总线，子级事件优先在子级执行并自动冒泡给父总线；子总线关闭自动脱钩父总线，父总线关闭级联关闭全部子总线。
- 注解监听字段在分发时重新读取，替换可变字段中的回调后无需重新注册。
- 参数化父类或接口中的泛型 `EventListener<T>` 字段会在扫描监听器类型时解析。
- 反射缓存与事件继承拓扑（`EVENT_HIERARCHIES`）基于进程级静态 `ClassValue`，跨总线实例共享扫描结果与动态 Lambda 调用工场，并在单类型层级上实现零集合分配快速路径。
- `metrics()` 可获取全局分发、实际调用、失败次数、总纳秒耗时与峰值纳秒耗时的不可变快照；`metrics(Event.class)` 可按运行时事件类型查看；`resetMetrics()` 只重置统计，不影响注册。
- `EventManager` 实现 `AutoCloseable`，`close()` 会清理所有监听器和缓存，可安全重复调用。
- `registeredEventTypes()` 返回已注册事件类型的只读快照，`isEmpty()` 可用于健康检查或停机判断。

## 构建与测试

```text
gradlew.bat clean build -PtestJavaVersion=8
gradlew.bat test -PtestJavaVersion=25
```

公开 API 不包含 Minecraft、Fabric、Forge 或其他第三方游戏特化类型，为 100% 纯 JDK 通用高性能事件框架。
