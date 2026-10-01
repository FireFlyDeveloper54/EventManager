package dev.hotaru.event;

import dev.hotaru.event.annotations.EventTarget;

import java.util.concurrent.TimeUnit;

/**
 * 快速上手示例：演示事件总线最常用的几种玩法。
 *
 * <p>运行方式：{@code ./gradlew demoRun}，或在 IDEA 中直接运行 main 方法。
 */
public class QuickStartDemo {

    // ========== 1. 事件就是普通类，实现 Event 标记接口即可 ==========

    public static class OrderCreated implements Event {
        final String orderId;
        final double amount;

        public OrderCreated(String orderId, double amount) {
            this.orderId = orderId;
            this.amount = amount;
        }
    }

    /** 可取消事件：前面的监听器取消后，ignoreCancelled 的监听器会被跳过。 */
    public static class OrderCancelRequest extends CancellableEvent {
        final String orderId;

        public OrderCancelRequest(String orderId) {
            this.orderId = orderId;
        }
    }

    public static class PaymentArrived implements Event {
        final String paymentId;

        public PaymentArrived(String paymentId) {
            this.paymentId = paymentId;
        }
    }

    // ========== 2. 注解式监听器：普通对象 + @EventTarget ==========

    public static class InventoryService {
        @EventTarget
        public void onOrderCreated(OrderCreated event) {
            System.out.println("[库存服务] 收到订单 " + event.orderId + "，准备扣减库存");
        }
    }

    public static void main(String[] args) throws Exception {
        EventManager bus = new EventManager("demo-bus");

        // 错误处理器：任何监听器抛异常时，总线会带着事件上下文调用它，业务不受影响
        bus.setErrorHandler(new EventErrorHandler() {
            @Override
            public void handle(Event event, Object listener, Throwable throwable) {
                System.out.println("[错误处理] " + event.getClass().getSimpleName()
                        + " 处理失败: " + throwable.getMessage());
            }
        });

        // 注册注解监听器
        bus.register(new InventoryService());

        // 流式 DSL：优先级 + 过滤 + 只触发一次，正交组合不打架
        // lambda 过滤器直接传即可；如需使用 EventFilter 实现类，请用 filterWith(...)
        java.util.function.Predicate<OrderCreated> bigOrder = e -> e.amount > 1000;
        bus.on(OrderCreated.class)
                .priority(Priority.HIGH)
                .filter(bigOrder)
                .once()
                .handle(e -> System.out.println("[风控] 大额订单 " + e.orderId
                        + "（" + e.amount + " 元）已标记复查（once：之后不再触发）"));

        // 故障隔离：这个监听器永远失败，但不影响同一事件的其他监听器
        bus.on(OrderCreated.class).handle(new java.util.function.Consumer<OrderCreated>() {
            @Override
            public void accept(OrderCreated event) {
                throw new RuntimeException("我坏了");
            }
        });

        System.out.println("=== 分发 OrderCreated ===");
        bus.dispatch(new OrderCreated("A-1001", 59.9));
        bus.dispatch(new OrderCreated("A-1002", 9999.0));

        System.out.println();
        System.out.println("=== 可取消事件 ===");
        bus.on(OrderCancelRequest.class).handle(e -> {
            e.setCancelled(true);
            System.out.println("[审核服务] 订单 " + e.orderId + " 的取消请求被驳回");
        });
        bus.on(OrderCancelRequest.class).ignoreCancelled()
                .handle(e -> System.out.println("[订单服务] 执行取消（取消被驳回，不应出现这行）"));
        bus.dispatch(new OrderCancelRequest("A-1001"));

        System.out.println();
        System.out.println("=== 粘性事件：先发布，后注册的监听器立即回放最近一次 ===");
        bus.dispatchSticky(new PaymentArrived("PAY-9527"));
        bus.on(PaymentArrived.class).sticky()
                .handle(e -> System.out.println("[对账服务] 注册即收到最近的支付回调: " + e.paymentId));

        System.out.println();
        System.out.println("=== 事务与 Saga 补偿：中途失败则缓冲事件全部丢弃，补偿逆序执行 ===");
        try {
            bus.transaction(tx -> {
                tx.onRollback(() -> System.out.println("[补偿 1] 释放预占库存"));
                tx.onRollback(() -> System.out.println("[补偿 2] 撤销冻结款"));
                bus.dispatch(new OrderCreated("TX-1", 1.0)); // 事务内分发只是缓冲，暂不投递
                throw new IllegalStateException("发货失败，回滚！");
            });
        } catch (RuntimeException ex) {
            System.out.println("[事务] 已回滚: " + ex.getMessage());
        }

        System.out.println();
        System.out.println("=== expect：异步代码里等待某个事件到达（带超时自动注销）===");
        bus.expect(PaymentArrived.class, null, 1, TimeUnit.SECONDS)
                .thenAccept(e -> System.out.println("[异步] 等到了支付回调 " + e.paymentId));
        Thread.sleep(100);
        bus.dispatch(new PaymentArrived("PAY-0001"));
        Thread.sleep(200);

        System.out.println();
        System.out.println("=== demo 结束 ===");
    }
}
