# EventManager

一个无任何第三方依赖（100% 纯 JDK 原生标准库实现，零编译期/运行时外部依赖）、基于注解与 Lambda 的高性能通用 Java 事件总线，全面兼容 JDK 8 至 JDK 25。

## 核心特性

- **极致性能**：基于 `ClassValue` 与 `LambdaMetafactory` 实现进程级动态编译与元数据复用；分发使用不可变快照，单类型事件零集合分配。
- **Project Loom 虚拟线程**：原生集成 JDK 21+ 虚拟线程（Virtual Threads）异步调度，支持单机百万级高并发事件处理；在 JDK 8-20 上自动优雅降级为平台线程池。
- **泛型事件精准派发**：原生支持 `GenericEvent<T>` 与 `TypeToken<T>`，彻底攻克 Java 运行时泛型擦除难题。支持泛型参数多态继承与通配符匹配（如 `OrderEvent<FoodOrder>` 与 `OrderEvent<BookOrder>` 互不干扰）。
- **粘性事件与即时回放 (Sticky Events)**：支持 `bus.dispatchSticky(event)` 缓存事件状态，并在新监听器（`@EventTarget(sticky = true)` 或 `.sticky()`）注册时立即精准重播最近一次的粘性事件，适用于配置变更、状态同步与跨生命周期订阅。
- **高频流量整流 (Throttling / Debouncing / Sampling)**：流式订阅者原生支持 `.throttle(interval, unit)`（无锁 CAS 限流）、`.debounce(delay, unit)`（静默期防抖）、`.sample(count)`（N 选 1 采样），轻松应对高频滚动、窗口变动、键盘输入与瞬时网络流量冲击。
- **处理器故障隔离与断路熔断器 (`CircuitBreaker`)**：无锁状态机（`CLOSED -> OPEN -> HALF_OPEN`），流式订阅者支持 `.circuitBreaker(maxFailures, cooldown, unit)`，在下游监听器持续崩溃或超时时自动切断派发进入冷静期，杜绝雪崩效应，保护系统整体稳定性。
- **JDK 9+ Flow.Publisher 原生响应式流适配**：原生支持 `bus.asFlowPublisher(Event.class)`，以纯原生动态代理机制导出符合 Reactive Streams 规范且具备背压需求控制（`request(n)`）的 `java.util.concurrent.Flow.Publisher<T>`，在 JDK 9+ 上零第三方库无缝衔接响应式生态。
- **微批处理与滑动窗口**：流式订阅者原生支持 `.buffer(maxBatchSize, timeout, unit).handleBatch(batch -> ...)`，按数量或静默超时自动聚合微批次并批量投递，彻底消除高通量日志与入库的 I/O 碎片化瓶颈。
- **监听器弹性自愈重试**：流式订阅者原生支持 `.retry(...)`，内置指数退避算法（`RetryPolicy.exponentialBackoff`）与可重试异常白名单过滤，轻松应对分布式网络抖动与瞬时偶发异常。
- **Saga 补偿性事务机制**：`bus.transaction(tx -> ...)` 原生提供 `tx.onRollback(Runnable compensation)`，当事务中途报错或显式回滚时，以 LIFO 逆序自动触发补偿动作，构建企业级 Saga 一致性闭环。
- **架构拓扑自省与图导出**：原生提供 `bus.exportTopology(TopologyFormat.MERMAID)` 与 `DOT` 格式导出，无需任何外部绘图库，一键将总线所有监听器、DAG 依赖、事件继承与 Upcaster 转换图输出为标准 Markdown 流程图。
- **时间旅行调试与事件录制重放**：提供轻量非侵入式 `EventRecorder`（`bus.startRecording()`），纳秒级捕获事件分发轨迹与上下文，生成不可变 `RecordedSession`，并支持仿真重放到隔离总线中实现时序确定性复现。
- **DAG 监听器拓扑依赖编排**：原生支持 `@EventTarget(after = ..., before = ..., afterClasses = ..., beforeClasses = ...)` 及流式 DSL `.after(...) / .before(...)`，采用 Kahn 算法进行拓扑排序，解决同级或跨优先级精细依赖，内置循环依赖检测并抛出 `CircularDependencyException`。
- **异步背压通道与有界环形缓冲**：新增 `AsyncEventChannel<T>`（`bus.createChannel(...)`），提供 `BLOCK`、`DROP_OLDEST`、`DROP_LATEST`、`CALLER_RUNS` 4 种生产级背压溢出策略，彻底杜绝突发洪峰流量下异步线程池无界排队引发的 OOM 风险。
- **事件模式演化与向上转换器**：原生支持 `EventUpcaster<S, T>` 与多跳级联转换链（如 `V1 -> V2 -> V3`），并在粘性事件（Sticky Events）派发与回放中自动同步升级，实现长周期架构下的事件无缝版本演进。
- **硬件级抗缓存行伪共享 (Anti-False-Sharing)**：在熔断器（`CircuitBreaker`）等高频并发核心状态上应用 64 字节 CPU 缓存行隔离填充（Padding），消除多核服务器缓存一致性总线风暴。
- **流式订阅 DSL**：提供 `bus.on(Event.class)` 流式构建器，正交解耦 `priority`、`filter`、`once`、`weak`、`sticky`、`throttle`、`debounce`、`sample`、`circuitBreaker` 与 `genericType`，消除传统重载方法的组合爆炸。
- **异步事件条件等待器**：原生支持 `bus.expect(...)`，以 `CompletableFuture` 异步等待满足特定条件的事件到达，内置超时熔断与自动注销机制。
- **分发上下文隐式传播**：提供 `EventContext` 元数据载体，支持在不侵入事件类的前提下隐式传递 TraceId、租户信息或自定义参数，并支持跨异步线程无缝传播。
- **调用方纳秒级回溯追踪**：内置 `EventTrace`，在 JDK 9+ 下利用 `StackWalker` 纳秒级捕获事件分发的精确代码行；自动赋能 `DeadEvent` 死信排查。
- **响应式流发布者**：提供 `bus.asPublisher(Event.class)`，以纯原生函数式支持响应式流订阅与链式过滤。
- **聚合异常传播策略**：新增 `ErrorPolicy.AGGREGATE`，分发时不阻断后续处理器，分发结束后利用原生 `Throwable.addSuppressed(...)` 将所有失败聚合为结构化 `EventDispatchException` 抛给调用方。
- **事务性事件缓冲与回滚**：原生提供线程隔离的事件事务（`bus.transaction(...)`），代码块正常结束原子批量 flush；若中途报错或显式回滚，所有缓冲事件自动丢弃（Rollback），杜绝业务脏状态副作用扩散。
- **无锁并发**：处理器增删全流程采用 CAS 无锁循环，杜绝全局粗粒度锁竞争与 `ConcurrentModificationException`。
- **内存安全**：原生支持弱引用托管（`registerWeak` / `subscribeWeak` / `.weak()`），提供主动死引用清理（`purgeDeadHandlers`），彻底消除长生命周期事件总线导致的 *Lapsed Listener* 内存残留。
- **死信检测**：原生支持 `DeadEvent` 未处理事件感知，防止复杂系统与插件架构中事件因时序颠倒而静默丢失。
- **声明式过滤**：支持 `@EventTarget(filter = MyFilter.class)`，以声明方式将事件过滤器绑定至监听器；支持逻辑运算与 JDK `Predicate` 无缝互通。
- **零分配拦截器**：提供 `EventInterceptor` 与 `EventInterceptors.chain` 管道组合器，基于轻量可重入执行帧栈（`DispatchFrame`）实现分发热路径 **0 堆对象分配（Zero Heap Allocation）**。
- **线程亲和性**：支持配置 `enforceThread(...)`，在非法线程派发时即时抛出明确诊断异常，彻底杜绝游戏/UI 主线程竞态。
- **分层作用域**：支持 `createChildBus()` 创建子总线，事件就地处理后向上冒泡，子模块关闭时一键解绑并释放全部资源。
- **行业生态对齐**：全面提供与 Guava / Spring 习惯一致的 `dispatch(...)`、`dispatchAsync(...)` 等一等公民方法别名。
- **可观察性**：支持总线命名与结构化诊断（`toString()`），内置 APM 延迟分析器，纳秒级记录各事件类型的累计总耗时、峰值耗时（Max Spike）与平均延迟。

## 进阶与常用用法

### 1. 流式订阅者 DSL (`bus.on(...)`)
```java
// 链式正交组合任意修饰符：高优先级 + 过滤谓词 + 只触发一次 + 弱引用托管
Subscription sub = bus.on(OrderPaidEvent.class)
        .priority(Priority.HIGHEST)
        .filter(event -> event.getAmount() > 1000)
        .ignoreCancelled()
        .once()
        .weak()
        .handle(event -> {
            System.out.println("大额订单单次处理: " + event.getOrderId());
        });

// 也可以随时手动注销
sub.unsubscribe();
```

### 2. Project Loom 虚拟线程 (Virtual Threads)
```java
// 在 JDK 21+ 下自动启用虚拟线程每个任务一个线程；在低版本 JDK 8-20 自动降级为通用池
EventManager bus = EventManager.builder()
        .useVirtualThreads(true)
        .build();

// 异步分发：完全无惧高延迟 I/O 监听器导致线程池耗尽
CompletableFuture<OrderEvent> future = bus.dispatchAsync(new OrderEvent(orderId));
```

### 3. 异步事件条件等待器 (`bus.expect(...)`)
```java
// 异步等待满足条件的下一个事件到达，超时时间 5 秒，到达后自动触发并注销监听器
CompletableFuture<PaymentResultEvent> future = bus.expect(
        PaymentResultEvent.class,
        event -> event.getPaymentId().equals("PAY-10086"),
        5,
        TimeUnit.SECONDS
);

future.thenAccept(result -> {
    System.out.println("支付回调成功: " + result.getStatus());
}).exceptionally(ex -> {
    System.err.println("等待支付回调超时或异常: " + ex);
    return null;
});
```

### 4. 分发上下文与元数据载体 (`EventContext`)
```java
// 隐式携带链路 TraceId 或租户信息，无需修改事件类结构
EventContext context = EventContext.of("traceId", "TR-778899")
        .with("tenantId", 101);

bus.dispatch(new UserLoginEvent("Alice"), context);

// 在任意监听器内部均可随时读取：
public class MyListener {
    @EventTarget
    public void onLogin(UserLoginEvent event) {
        String traceId = EventContext.current().get("traceId", String.class);
        System.out.printf("[%s] 用户登录: %s%n", traceId, event.getUsername());
    }
}
```

### 5. 原生泛型事件精准匹配 (`GenericEvent<T>` + `TypeToken<T>`)
```java
// 定义携带泛型信息的事件（继承 AbstractGenericEvent 自动提取类型参数，或传入 Type）
public class MessageEvent<T> extends AbstractGenericEvent<T> {
    private final T content;
    public MessageEvent(T content, Type genericType) {
        super(genericType);
        this.content = content;
    }
    public T getContent() { return content; }
}

public class MyListener {
    // 仅接收 String 泛型的 MessageEvent
    @EventTarget
    public void onTextMessage(MessageEvent<String> event) {
        System.out.println("收到文本: " + event.getContent().toUpperCase());
    }

    // 仅接收 Integer 泛型的 MessageEvent
    @EventTarget
    public void onNumberMessage(MessageEvent<Integer> event) {
        System.out.println("收到数字: " + (event.getContent() + 1));
    }

    // 接收所有 Number 及子类（Integer, Double 等）的多态泛型监听器
    @EventTarget
    public void onAnyNumber(MessageEvent<Number> event) {}

    // 未指定具体泛型的 Raw 监听器接收全部泛型消息
    @EventTarget
    public void onRawMessage(MessageEvent event) {}
}

// 分发时精准按具体泛型路由，绝不会将 Integer 派发给 String 监听器导致 ClassCastException
bus.dispatch(new MessageEvent<>("Hello", String.class));
bus.dispatch(new MessageEvent<>(12345, Integer.class));
```

### 6. 原生响应式流发布者 (`EventPublisher`)
```java
EventPublisher<SensorEvent> publisher = bus.asPublisher(SensorEvent.class);

// 函数式链式过滤与订阅
Subscription sub = publisher
        .filter(e -> e.getTemperature() > 80.0)
        .subscribe(e -> System.out.println("高温警报: " + e.getTemperature()));
```

### 7. 事务性事件缓冲与异常回滚 (`bus.transaction(...)`)
```java
try {
    bus.transaction(() -> {
        bus.dispatch(new OrderCreatedEvent(orderId));
        
        // 步骤2：扣减库存
        inventoryService.deduct(orderId);
        bus.dispatch(new InventoryDeductedEvent(orderId));
        
        // 步骤3：抛出异常导致事务失败
        if (paymentFailed) {
            throw new RuntimeException("扣款失败");
        }
        
        bus.dispatch(new OrderFinishedEvent(orderId));
    });
} catch (RuntimeException e) {
    // 捕获异常：整个事务内暂存的全部事件已被原子丢弃（Rollback），没有任何监听器被触发！
}
```

### 8. 聚合异常传播策略 (`ErrorPolicy.AGGREGATE`)
```java
EventManager bus = EventManager.builder()
        .errorPolicy(ErrorPolicy.AGGREGATE)
        .build();

// 当注册了多个可能抛出异常的监听器时：
// 1. 所有监听器都会被尽可能完整执行，绝不会因前面报错而中止后续监听器；
// 2. 分发结束时，若存在失败，将抛出单一 EventDispatchException；
// 3. 所有后续异常均通过 Java 原生 Throwable.addSuppressed 关联，调用方可完全排查：
try {
    bus.dispatch(new SyncEvent());
} catch (EventDispatchException ex) {
    System.err.println("主异常: " + ex.getCause());
    for (Throwable suppressed : ex.getSuppressed()) {
        System.err.println("聚合抑制异常: " + suppressed);
    }
}
```

### 9. 调用方纳秒级回溯追踪 (`DeadEvent` & `EventTrace`)
```java
bus.register(DeadEvent.class, dead -> {
    EventTrace trace = dead.getTrace();
    System.out.printf("死信未处理事件 [%s]，发生于: %s.%s(%s:%d)%n",
            dead.getEvent().getClass().getSimpleName(),
            trace.getClassName(),
            trace.getMethodName(),
            trace.getFileName(),
            trace.getLineNumber());
});
```

### 10. 拦截器管道（零堆分配热路径）
```java
// 全局记录所有事件派发耗时与审计日志
bus.addInterceptor((event, proceed) -> {
    long start = System.nanoTime();
    try {
        proceed.run(); // 继续执行；不调用则短路拦截
    } finally {
        long cost = System.nanoTime() - start;
        System.out.printf("[%s] 派发耗时: %d ns%n", event.getClass().getSimpleName(), cost);
    }
});
```

### 11. 分层/子作用域总线（模块隔离）
```java
EventManager childBus = bus.createChildBus("ChatModule");
childBus.register(new ChatListener());

// 子总线分发的事件在子级处理完毕后，自动冒泡至父级总线
childBus.dispatch(new ChatMessageEvent("hi"));

// 模块卸载：一键关闭子总线并释放所有子级监听器，不影响父总线
childBus.close();
```

### 12. 粘性事件与状态即时回放 (Sticky Events)
```java
// 派发并持久缓存当前最新状态事件
bus.dispatchSticky(new ConfigLoadedEvent("dark_theme", 42));

// 稍后注册的监听器（无论注解还是流式 DSL）将立即收到上一次派发的粘性事件进行状态同步！
bus.on(ConfigLoadedEvent.class)
        .sticky()
        .handle(event -> {
            System.out.println("即时获取到历史配置: " + event.getKey());
        });

// 也可以直接获取或显式清除
ConfigLoadedEvent last = bus.getSticky(ConfigLoadedEvent.class);
bus.removeSticky(ConfigLoadedEvent.class);
```

### 13. 高频流量整形：节流 (Throttle)、防抖 (Debounce) 与采样 (Sample)
```java
// 1. 节流：在 100 毫秒内最多触发 1 次，期间多余的高频事件自动丢弃（基于 Lock-free CAS）
bus.on(WindowResizeEvent.class)
        .throttle(100, TimeUnit.MILLISECONDS)
        .handle(e -> updateLayout(e.getWidth(), e.getHeight()));

// 2. 防抖：停止输入达到 300 毫秒的静默期后才真正派发最后一次有效事件
bus.on(SearchInputEvent.class)
        .debounce(300, TimeUnit.MILLISECONDS)
        .handle(e -> executeSearch(e.getQuery()));

// 3. 采样：每 10 次事件仅采集派发 1 次（如高频传感器数据记录）
bus.on(MetricsSampleEvent.class)
        .sample(10)
        .handle(e -> recordMetrics(e));
```

### 14. 故障隔离与断路熔断器 (`CircuitBreaker`)
```java
// 为下游可能发生故障/超时的监听器添加断路器保护：
// 若在短时间内连续发生 3 次异常，断路器进入 OPEN 状态，在 5 秒冷却期内自动短路所有调用；
// 冷却期结束后进入 HALF-OPEN 探测，成功则自动自愈恢复 CLOSED！
bus.on(ExternalApiSyncEvent.class)
        .circuitBreaker(3, 5, TimeUnit.SECONDS)
        .handle(e -> callUnstableRemoteService(e));
```

### 15. JDK 9+ 原生 Flow.Publisher 响应式流适配（支持背压）
```java
// 在 JDK 9+ 下直接无缝导出 java.util.concurrent.Flow.Publisher
// 零依赖，通过原生 Subscription.request(n) 进行背压需求控制
Object publisher = bus.asFlowPublisher(StockPriceEvent.class);
// 配合 Flow.Subscriber 即可实现纯原生的背压消费与流式响应式编程！
```

### 16. DAG 监听器拓扑依赖编排与循环依赖自检
```java
// 声明精细的先后依赖关系，无论是通过类还是自定义 ID
public class SecurityListener {
    @EventTarget(id = "security", afterClasses = {AuthListener.class})
    public void onEvent(UserActionEvent event) {
        System.out.println("2. 安全权限校验完毕");
    }
}

public class AuditListener {
    @EventTarget(id = "audit", after = {"security"})
    public void onEvent(UserActionEvent event) {
        System.out.println("3. 审计日志持久化");
    }
}

// 也可以通过流式 DSL 自由编排：
bus.on(UserActionEvent.class)
        .id("worker")
        .after("security")
        .before("audit")
        .handle(event -> System.out.println("2.5 核心业务逻辑执行"));

// 总线在注册时自动执行 Kahn 拓扑排序；若存在 A -> B -> A 环形循环依赖，将即时抛出 CircularDependencyException 并输出闭环路径！
```

### 17. 异步背压缓冲通道 (`AsyncEventChannel` & `BackpressurePolicy`)
```java
// 创建容量为 1000 的异步事件通道，配置背压策略为丢弃最旧事件 (DROP_OLDEST)
AsyncEventChannel<SensorDataEvent> channel = bus.createChannel(
        SensorDataEvent.class,
        1000,
        BackpressurePolicy.DROP_OLDEST
);

// 生产者极速发布：
channel.publish(new SensorDataEvent(sensorId, value));

// 随时观测通道积压与丢弃健康指标：
System.out.printf("积压中: %d, 成功派发: %d, 背压丢弃: %d%n",
        channel.getPendingCount(),
        channel.getDispatchedCount(),
        channel.getDroppedCount());

// 模块卸载时优雅关闭通道：
channel.close();
```

### 18. 事件版本模式演化与向上转换器 (`EventUpcaster`)
```java
// 注册 V1 -> V2 的转换适配器
bus.registerUpcaster(OrderCreatedV1.class, OrderCreatedV2.class, v1 -> {
    return new OrderCreatedV2(v1.getOrderId(), 100.0);
});

// 注册 V2 -> V3 的转换适配器（自动组合成 V1 -> V2 -> V3 多跳链路）
bus.registerUpcaster(OrderCreatedV2.class, OrderCreatedV3.class, v2 -> {
    return new OrderCreatedV3(v2.getOrderId(), v2.getAmount(), "CNY");
});

// 业务只需派发老版本的 OrderCreatedV1：
bus.dispatch(new OrderCreatedV1("ORD-10086"));

// 订阅 OrderCreatedV1, OrderCreatedV2, OrderCreatedV3 的监听器均能按需接收到各自版本！
// 粘性事件（Sticky Events）亦自动级联升级缓存，新注册的 V3 监听器能立刻获取到 V1 派发后自动升版的粘性数据！
```

### 19. 高吞吐微批处理滑动窗口 (`.buffer(...)` / `.handleBatch(...)`)
```java
// 收集高频日志：当积压达到 100 条，或者等待静默期达到 50 毫秒时，打包为一个 List 批量派发
bus.on(LogItemEvent.class)
        .buffer(100, 50, TimeUnit.MILLISECONDS)
        .handleBatch(batch -> {
            database.batchInsert(batch); // 批量落盘，I/O 吞吐提升数十倍！
        });
```

### 20. 监听器故障重试与指数退避 (`.retry(...)`)
```java
// 配置最多重试 4 次，初始等待 10ms，最大等待 100ms 的指数退避策略，且仅在捕获 IOException 时重试
RetryPolicy policy = RetryPolicy.exponentialBackoff(4, 10, 100, TimeUnit.MILLISECONDS)
        .retryOn(IOException.class);

bus.on(RemoteSyncEvent.class)
        .retry(policy)
        .handle(event -> syncService.push(event));
```

### 21. Saga 补偿事务模式 (`tx.onRollback`)
```java
bus.transaction(tx -> {
    // 步骤 1：扣减账户余额
    bus.dispatch(new DeductBalanceEvent(userId, 500));
    // 注册发生失败时的逆向补偿动作：
    tx.onRollback(() -> bus.dispatch(new RefundBalanceEvent(userId, 500)));

    // 步骤 2：发货服务若抛出异常导致事务中止：
    shippingService.dispatchItem(itemId);
});
// 事务异常：不仅未派发的事件被丢弃，已注册的补偿事件立即以 LIFO 逆序自动触发执行！
```

### 22. 架构全拓扑自省与 Mermaid 图导出 (`exportTopology`)
```java
// 一键导出全总线监听器拓扑图（Mermaid 格式，可直接在 GitHub / Notion / Markdown 中渲染）
String mermaidMarkdown = bus.exportTopology(TopologyFormat.MERMAID);
System.out.println(mermaidMarkdown);

// 亦可导出为 Graphviz DOT 格式：
String dotGraph = bus.exportTopology(TopologyFormat.DOT);
```

### 23. 时间旅行录制与仿真重播 (`EventRecorder` & `RecordedSession`)
```java
// 1. 开启无侵入事件录制器
EventRecorder recorder = bus.startRecording();

// 2. 运行复杂的业务或测试逻辑...
doComplexBusinessOperations();

// 3. 停止录制并获取时序轨迹快照
RecordedSession session = recorder.stop();
System.out.println("录制到的事件总数: " + session.size());

// 4. 在全新的隔离测试总线中以原始相对时间间隔或极速模式重播：
EventManager mockBus = new EventManager();
session.replayTo(mockBus); // 完全无损重现当时的事件链路与上下文！
```
