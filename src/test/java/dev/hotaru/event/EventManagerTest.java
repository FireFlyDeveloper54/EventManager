package dev.hotaru.event;

import dev.hotaru.event.annotations.EventTarget;
import dev.hotaru.event.impl.CancellableEvent;
import dev.hotaru.event.impl.CancellableStoppableEvent;
import dev.hotaru.event.impl.Event;
import dev.hotaru.event.impl.StoppableEvent;
import lombok.Getter;
import lombok.Setter;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EventManagerTest {

    @Test
    void registeringTwiceInvokesHandlerOnce() {
        EventManager events = new EventManager();
        CountingListener listener = new CountingListener();

        events.register(listener);
        events.register(listener);
        events.call(new Ping());

        assertEquals(1, listener.getPings());
        events.unregister(listener);
        events.call(new Ping());
        assertEquals(1, listener.getPings());
    }

    @Test
    void twoListenersBothReceiveTheEvent() {
        EventManager events = new EventManager();
        CountingListener first = new CountingListener();
        CountingListener second = new CountingListener();

        events.register(first);
        events.register(second);
        events.call(new Ping());

        assertEquals(1, first.getPings());
        assertEquals(1, second.getPings());
    }

    @Test
    void hierarchyDispatchIncludesSupertypeHandlers() {
        EventManager events = new EventManager();
        CountingListener listener = new CountingListener();
        events.register(listener);

        events.call(new AdminPing());

        assertEquals(1, listener.getPings());
        assertEquals(1, listener.getAdminPings());
        assertEquals(2, events.handlerCount(AdminPing.class));
        assertEquals(1, events.exactHandlerCount(AdminPing.class));
    }

    @Test
    void exactDispatchSkipsSupertypeHandlers() {
        EventManager events = new EventManager();
        CountingListener listener = new CountingListener();
        events.register(listener);

        events.callExact(new AdminPing());

        assertEquals(0, listener.getPings());
        assertEquals(1, listener.getAdminPings());
    }

    @Test
    void priorityAndRegistrationOrderAreStable() {
        EventManager events = new EventManager();
        List<String> order = new ArrayList<String>();

        events.register(Ping.class, Priority.NORMAL, new Consumer<Ping>() {
            @Override
            public void accept(Ping ping) {
                order.add("second-normal");
            }
        });
        events.register(Ping.class, Priority.HIGH, new Consumer<Ping>() {
            @Override
            public void accept(Ping ping) {
                order.add("high");
            }
        });
        events.register(Ping.class, Priority.NORMAL, new Consumer<Ping>() {
            @Override
            public void accept(Ping ping) {
                order.add("third-normal");
            }
        });

        events.call(new Ping());
        assertEquals("high", order.get(0));
        assertEquals("second-normal", order.get(1));
        assertEquals("third-normal", order.get(2));
    }

    @Test
    void ignoreCancelledSkipsLaterHandlers() {
        EventManager events = new EventManager();
        AtomicInteger cancelledAware = new AtomicInteger();
        AtomicInteger always = new AtomicInteger();

        events.register(Save.class, Priority.HIGHEST, new Consumer<Save>() {
            @Override
            public void accept(Save event) {
                event.cancel();
            }
        });
        events.register(Save.class, Priority.NORMAL, true, new Consumer<Save>() {
            @Override
            public void accept(Save event) {
                cancelledAware.incrementAndGet();
            }
        });
        events.register(Save.class, Priority.LOW, false, new Consumer<Save>() {
            @Override
            public void accept(Save event) {
                always.incrementAndGet();
            }
        });

        Save event = events.call(new Save());
        assertTrue(event.isCancelled());
        assertEquals(0, cancelledAware.get());
        assertEquals(1, always.get());
    }

    @Test
    void stoppableEventAbortsDispatch() {
        EventManager events = new EventManager();
        AtomicInteger later = new AtomicInteger();

        events.register(Halt.class, Priority.HIGHEST, new Consumer<Halt>() {
            @Override
            public void accept(Halt event) {
                event.stop();
            }
        });
        events.register(Halt.class, Priority.NORMAL, new Consumer<Halt>() {
            @Override
            public void accept(Halt event) {
                later.incrementAndGet();
            }
        });

        events.call(new Halt());
        assertEquals(0, later.get());
    }

    @Test
    void cancellableStoppableEventStopsAfterCancel() {
        EventManager events = new EventManager();
        AtomicInteger later = new AtomicInteger();

        events.register(Abort.class, Priority.HIGHEST, new Consumer<Abort>() {
            @Override
            public void accept(Abort event) {
                event.cancel();
            }
        });
        events.register(Abort.class, Priority.NORMAL, false, new Consumer<Abort>() {
            @Override
            public void accept(Abort event) {
                later.incrementAndGet();
            }
        });

        Abort event = events.call(new Abort());
        assertTrue(event.isCancelled());
        assertTrue(event.isStopped());
        assertEquals(0, later.get());
    }

    @Test
    void subscribeUnregistersAnnotatedListener() {
        EventManager events = new EventManager();
        CountingListener listener = new CountingListener();

        Subscription subscription = events.subscribe(listener);
        assertTrue(subscription.isSubscribed());
        events.call(new Ping());
        assertEquals(1, listener.getPings());

        subscription.unsubscribe();
        subscription.unsubscribe();
        assertFalse(subscription.isSubscribed());
        events.call(new Ping());
        assertEquals(1, listener.getPings());
    }

    @Test
    void unregisterSingleMethodLeavesOtherHandlers() throws Exception {
        EventManager events = new EventManager();
        CountingListener listener = new CountingListener();
        events.register(listener);

        Method ping = CountingListener.class.getDeclaredMethod("onPing", Ping.class);
        events.unregister(listener, ping);

        events.call(new Ping());
        events.call(new AdminPing());
        assertEquals(0, listener.getPings());
        assertEquals(1, listener.getAdminPings());
    }

    @Test
    void registerSingleMethodDoesNotRegisterSiblings() throws Exception {
        EventManager events = new EventManager();
        CountingListener listener = new CountingListener();
        Method ping = CountingListener.class.getDeclaredMethod("onPing", Ping.class);

        events.register(listener, ping);
        events.call(new Ping());
        events.call(new AdminPing());

        assertEquals(2, listener.getPings());
        assertEquals(0, listener.getAdminPings());
    }

    @Test
    void fieldListenerRunsAndAccessibilityIsRestored() throws Exception {
        EventManager events = new EventManager();
        FieldListener listener = new FieldListener();
        Field field = FieldListener.class.getDeclaredField("onPing");
        assertFalse(field.isAccessible());

        events.register(listener);
        assertFalse(field.isAccessible());

        events.call(new Ping());
        assertEquals(1, listener.getCalls().get());

        events.unregister(listener, field);
        events.call(new Ping());
        assertEquals(1, listener.getCalls().get());
    }

    @Test
    void staticClassRegistration() {
        EventManager events = new EventManager();
        StaticHandlers.getPings().set(0);

        events.register(StaticHandlers.class);
        events.call(new Ping());
        assertEquals(1, StaticHandlers.getPings().get());

        events.unregister(StaticHandlers.class, Ping.class);
        events.call(new Ping());
        assertEquals(1, StaticHandlers.getPings().get());
    }

    @Test
    void classArrivingThroughObjectParameterStillBindsStaticHandlers() {
        EventManager events = new EventManager();
        StaticHandlers.getPings().set(0);
        Object asObject = StaticHandlers.class;

        events.register(asObject);
        events.call(new Ping());

        assertEquals(1, StaticHandlers.getPings().get());
        assertTrue(events.isRegistered(StaticHandlers.class));
    }

    @Test
    void varargsRegistrationBindsEveryClass() {
        EventManager events = new EventManager();
        StaticHandlers.getPings().set(0);
        MoreStaticHandlers.getPings().set(0);

        events.register(StaticHandlers.class, MoreStaticHandlers.class);
        events.call(new Ping());

        assertEquals(1, StaticHandlers.getPings().get());
        assertEquals(1, MoreStaticHandlers.getPings().get());
    }

    @Test
    void clearRemovesHandlersAndStaysUsable() {
        EventManager events = new EventManager();
        events.register(new CountingListener());
        events.register(new FieldListener());
        events.call(new Ping());

        events.clear();

        assertEquals(0, events.handlerCount());

        CountingListener rebound = new CountingListener();
        events.register(rebound);
        events.call(new Ping());
        assertEquals(1, rebound.getPings());
    }

    @Test
    void lazySupplierIsNotInvokedWithoutListeners() {
        EventManager events = new EventManager();
        AtomicBoolean created = new AtomicBoolean();

        Ping result = events.call(Ping.class, new Supplier<Ping>() {
            @Override
            public Ping get() {
                created.set(true);
                return new Ping();
            }
        });

        assertNull(result);
        assertFalse(created.get());
    }

    @Test
    void exceptionsAreIsolatedAndUnwrapped() {
        EventManager events = new EventManager();
        AtomicReference<Throwable> seen = new AtomicReference<Throwable>();
        events.setErrorHandler(new EventErrorHandler() {
            @Override
            public void handle(Event event, Object listener, Throwable throwable) {
                seen.set(throwable);
            }
        });
        AtomicInteger later = new AtomicInteger();

        events.register(Ping.class, new Consumer<Ping>() {
            @Override
            public void accept(Ping ping) {
                throw new IllegalStateException("boom");
            }
        });
        events.register(Ping.class, new Consumer<Ping>() {
            @Override
            public void accept(Ping ping) {
                later.incrementAndGet();
            }
        });

        events.call(new Ping());
        assertTrue(seen.get() instanceof IllegalStateException);
        assertEquals("boom", seen.get().getMessage());
        assertEquals(1, later.get());
    }

    @Test
    void stopPolicyReportsAndStopsTheCurrentDispatch() {
        EventManager events = new EventManager();
        events.setErrorPolicy(ErrorPolicy.STOP);
        AtomicInteger later = new AtomicInteger();
        events.register(Ping.class, new Consumer<Ping>() {
            @Override
            public void accept(Ping ping) {
                throw new IllegalStateException("stop");
            }
        });
        events.register(Ping.class, new Consumer<Ping>() {
            @Override
            public void accept(Ping ping) {
                later.incrementAndGet();
            }
        });

        events.call(new Ping());
        assertEquals(0, later.get());
    }

    @Test
    void propagatePolicyRethrowsRuntimeFailures() {
        EventManager events = new EventManager();
        events.setErrorPolicy(ErrorPolicy.PROPAGATE);
        final IllegalArgumentException expected = new IllegalArgumentException("propagate");
        AtomicReference<Throwable> seen = new AtomicReference<Throwable>();
        events.setErrorHandler(new EventErrorHandler() {
            @Override
            public void handle(Event event, Object listener, Throwable throwable) {
                seen.set(throwable);
            }
        });
        events.register(Ping.class, new Consumer<Ping>() {
            @Override
            public void accept(Ping ping) {
                throw expected;
            }
        });

        IllegalArgumentException actual = assertThrows(IllegalArgumentException.class,
                () -> events.call(new Ping()));
        assertSame(expected, actual);
        assertSame(expected, seen.get());
    }

    @Test
    void brokenStoppableStateIsReportedOnlyOnceInContinueMode() {
        EventManager events = new EventManager();
        AtomicInteger failures = new AtomicInteger();
        AtomicInteger calls = new AtomicInteger();
        events.setErrorHandler((event, listener, throwable) -> failures.incrementAndGet());
        events.register(BrokenHalt.class, event -> calls.incrementAndGet());
        events.register(BrokenHalt.class, event -> calls.incrementAndGet());

        events.call(new BrokenHalt());

        assertEquals(1, failures.get());
        assertEquals(2, calls.get());
    }

    @Test
    void brokenCancellableStateDoesNotSuppressIgnoredHandlersInContinueMode() {
        EventManager events = new EventManager();
        AtomicInteger failures = new AtomicInteger();
        AtomicInteger calls = new AtomicInteger();
        events.setErrorHandler((event, listener, throwable) -> failures.incrementAndGet());
        events.register(BrokenCancel.class, Priority.NORMAL, true, event -> calls.incrementAndGet());
        events.register(BrokenCancel.class, Priority.NORMAL, true, event -> calls.incrementAndGet());

        events.call(new BrokenCancel());

        assertEquals(1, failures.get());
        assertEquals(2, calls.get());
    }

    @Test
    void eventSubscriberCanOptOutWithoutUnregistering() {
        EventManager events = new EventManager();
        ToggleListener listener = new ToggleListener();
        events.register(listener);

        listener.setEnabled(false);
        events.call(new Ping());
        assertEquals(0, listener.getPings());

        listener.setEnabled(true);
        events.call(new Ping());
        assertEquals(1, listener.getPings());
        assertTrue(events.isRegistered(listener));
    }

    @Test
    void afterDispatchRunsEvenWithoutHandlers() {
        EventManager events = new EventManager();
        AtomicInteger completed = new AtomicInteger();
        events.call(new Ping(), new Runnable() {
            @Override
            public void run() {
                completed.incrementAndGet();
            }
        });
        assertEquals(1, completed.get());
    }

    @Test
    void asyncDispatchUsesTheProvidedExecutor() throws Exception {
        EventManager events = new EventManager();
        AtomicInteger calls = new AtomicInteger();
        AtomicBoolean executed = new AtomicBoolean();
        Executor directExecutor = command -> {
            executed.set(true);
            command.run();
        };
        Ping event = new Ping();
        events.register(Ping.class, ping -> calls.incrementAndGet());

        CompletableFuture<Ping> completion = events.callAsync(event, directExecutor);

        assertSame(event, completion.get());
        assertTrue(executed.get());
        assertEquals(1, calls.get());
    }

    @Test
    void compositeSubscriptionAttemptsEveryCleanup() {
        AtomicInteger cleaned = new AtomicInteger();
        final RuntimeException expected = new RuntimeException("cleanup");
        Subscription failing = new Subscription() {
            @Override
            public void unsubscribe() {
                throw expected;
            }
        };
        Subscription succeeding = new Subscription() {
            @Override
            public void unsubscribe() {
                cleaned.incrementAndGet();
            }
        };

        Subscription composite = failing.and(succeeding);
        RuntimeException actual = assertThrows(RuntimeException.class, composite::unsubscribe);

        assertSame(expected, actual);
        assertEquals(1, cleaned.get());
        assertFalse(composite.isSubscribed());
    }

    @Test
    void onceRegistrationUnsubscribesBeforeInvocation() {
        EventManager events = new EventManager();
        AtomicInteger calls = new AtomicInteger();
        events.registerOnce(Ping.class, event -> {
            calls.incrementAndGet();
            events.call(new Ping());
        });

        events.call(new Ping());
        events.call(new Ping());

        assertEquals(1, calls.get());
        assertEquals(0, events.handlerCount());
    }

    @Test
    void filteredRegistrationOnlyInvokesAcceptedEvents() {
        EventManager events = new EventManager();
        AtomicInteger calls = new AtomicInteger();
        events.registerFiltered(Ping.class, event -> event.accept, event -> calls.incrementAndGet());

        Ping rejected = new Ping();
        rejected.accept = false;
        Ping accepted = new Ping();
        accepted.accept = true;
        events.call(rejected);
        events.call(accepted);

        assertEquals(1, calls.get());
    }

    @Test
    void metricsTrackDispatchesInvocationsAndFailures() {
        EventManager events = new EventManager();
        events.setErrorHandler((event, listener, throwable) -> {
        });
        events.register(Ping.class, event -> {
        });
        events.register(Ping.class, event -> {
            throw new IllegalStateException("metrics");
        });

        events.call(new Ping());
        EventMetrics metrics = events.metrics();
        assertEquals(1L, metrics.getDispatchedEvents());
        assertEquals(2L, metrics.getHandlerInvocations());
        assertEquals(1L, metrics.getFailures());

        events.resetMetrics();
        assertEquals(0L, events.metrics().getDispatchedEvents());
        assertEquals(0L, events.metrics().getHandlerInvocations());
        assertEquals(0L, events.metrics().getFailures());
    }

    @Test
    void metricsCanBeReadPerRuntimeEventType() {
        EventManager events = new EventManager();
        events.register(Ping.class, event -> {
        });
        events.register(AdminPing.class, event -> {
        });

        events.call(new Ping());
        events.call(new AdminPing());

        assertEquals(1L, events.metrics(Ping.class).getDispatchedEvents());
        assertEquals(1L, events.metrics(Ping.class).getHandlerInvocations());
        assertEquals(1L, events.metrics(AdminPing.class).getDispatchedEvents());
        assertEquals(2L, events.metrics(AdminPing.class).getHandlerInvocations());
    }

    @Test
    void closeClearsRegistrationsAndIsIdempotent() {
        EventManager events = new EventManager();
        events.register(Ping.class, event -> {
        });

        events.close();
        events.close();

        assertEquals(0, events.handlerCount());
        assertFalse(events.hasListeners(Ping.class));
    }

    @Test
    void registeredEventTypesReturnsReadOnlySnapshot() {
        EventManager events = new EventManager();
        events.register(Ping.class, event -> {
        });

        java.util.Set<Class<? extends Event>> types = events.registeredEventTypes();
        assertTrue(types.contains(Ping.class));
        assertFalse(events.isEmpty());
        assertThrows(UnsupportedOperationException.class, () -> types.clear());

        events.clear();
        assertTrue(events.isEmpty());
        assertTrue(types.contains(Ping.class));
    }

    @Test
    void overridingMethodShadowsInheritedHandler() {
        EventManager events = new EventManager();
        ChildListener child = new ChildListener();
        events.register(child);

        events.call(new Ping());
        assertEquals(1, child.getChildPings());
        assertEquals(0, child.getParentPings());
    }

    @Test
    void dispatchSnapshotIgnoresHandlersRegisteredDuringCall() {
        final EventManager events = new EventManager();
        AtomicInteger late = new AtomicInteger();

        events.register(Ping.class, new Consumer<Ping>() {
            @Override
            public void accept(Ping ping) {
                events.register(Ping.class, new Consumer<Ping>() {
                    @Override
                    public void accept(Ping nested) {
                        late.incrementAndGet();
                    }
                });
            }
        });

        events.call(new Ping());
        assertEquals(0, late.get());
        events.call(new Ping());
        assertEquals(1, late.get());
    }

    @Test
    void dispatchSnapshotSkipsHandlersUnregisteredDuringCall() {
        final EventManager events = new EventManager();
        final AtomicInteger removed = new AtomicInteger();
        final AtomicReference<Subscription> later = new AtomicReference<Subscription>();

        events.register(Ping.class, new Consumer<Ping>() {
            @Override
            public void accept(Ping ping) {
                later.get().unsubscribe();
            }
        });
        later.set(events.register(Ping.class, new Consumer<Ping>() {
            @Override
            public void accept(Ping ping) {
                removed.incrementAndGet();
            }
        }));

        events.call(new Ping());

        assertEquals(0, removed.get());
    }

    @Test
    void privateHandlersAreInvoked() {
        EventManager events = new EventManager();
        PrivateListener listener = new PrivateListener();
        events.register(listener);
        events.call(new Ping());
        assertEquals(1, listener.getPings());
    }

    @Test
    void explicitMemberRegistrationRequiresDeclaringClassMatch() throws Exception {
        EventManager events = new EventManager();
        CountingListener listener = new CountingListener();
        Method unrelated = SameSignature.class.getDeclaredMethod("onPing", Ping.class);

        events.register(listener, unrelated);
        events.call(new Ping());

        assertEquals(0, listener.getPings());
    }

    @Test
    void secondSubscribeDoesNotOwnAnExistingRegistration() {
        EventManager events = new EventManager();
        CountingListener listener = new CountingListener();
        Subscription first = events.subscribe(listener);
        Subscription second = events.subscribe(listener);

        assertTrue(first.isSubscribed());
        assertFalse(second.isSubscribed());
        second.unsubscribe();
        events.call(new Ping());
        assertEquals(1, listener.getPings());

        first.unsubscribe();
    }

    @Test
    void exactAndAssignableClassUnregisterAreDistinct() {
        EventManager events = new EventManager();
        ParentListener parent = new ParentListener();
        ChildListener child = new ChildListener();
        events.register(parent, child);

        events.unregisterExact(ParentListener.class);
        events.call(new Ping());
        assertEquals(0, parent.getParentPings());
        assertEquals(1, child.getChildPings());

        events.unregisterAssignable(ParentListener.class);
        events.call(new Ping());
        assertEquals(1, child.getChildPings());
    }
    @Test
    void mutableFieldListenerUsesCurrentCallback() {
        EventManager events = new EventManager();
        MutableFieldListener listener = new MutableFieldListener();
        events.register(listener);

        events.call(new Ping());
        listener.onPing = event -> listener.second.incrementAndGet();
        events.call(new Ping());

        assertEquals(1, listener.first.get());
        assertEquals(1, listener.second.get());
    }

    @Test
    void genericSuperclassFieldListenerTypeIsResolved() {
        EventManager events = new EventManager();
        GenericPingListener listener = new GenericPingListener();
        events.register(listener);

        events.call(new Ping());

        assertEquals(1, listener.calls.get());
    }

    @Test
    void handlerScanOrderIsDeterministicForEqualPriority() {
        EventManager events = new EventManager();
        OrderedListener listener = new OrderedListener();
        events.register(listener);

        events.call(new Ping());

        assertEquals(java.util.Arrays.asList("alpha", "zeta"), listener.order);
    }
    static final class SameSignature {
        @EventTarget
        public void onPing(Ping event) {
        }
    }

    static final class MutableFieldListener {
        private final AtomicInteger first = new AtomicInteger();
        private final AtomicInteger second = new AtomicInteger();

        @EventTarget
        private EventListener<Ping> onPing = event -> first.incrementAndGet();
    }

    static final class OrderedListener {
        private final List<String> order = new ArrayList<String>();

        @EventTarget
        public void zeta(Ping event) {
            order.add("zeta");
        }

        @EventTarget
        public void alpha(Ping event) {
            order.add("alpha");
        }
    }

    static class GenericFieldListener<T extends Event> {
        @EventTarget
        protected EventListener<T> listener;
    }

    static final class GenericPingListener extends GenericFieldListener<Ping> {
        private final AtomicInteger calls = new AtomicInteger();

        GenericPingListener() {
            listener = event -> calls.incrementAndGet();
        }
    }
    static class Ping implements Event {
        private boolean accept;
    }

    static final class AdminPing extends Ping {
    }

    static final class Save extends CancellableEvent {
    }

    static final class Halt extends StoppableEvent {
    }

    static final class BrokenHalt implements Event, dev.hotaru.event.impl.Stoppable {
        @Override
        public boolean isStopped() {
            throw new IllegalStateException("broken stopped state");
        }

        @Override
        public void setStopped(boolean state) {
        }
    }

    static final class BrokenCancel implements Event, dev.hotaru.event.impl.Cancellable {
        @Override
        public boolean isCancelled() {
            throw new IllegalStateException("broken cancelled state");
        }

        @Override
        public void setCancelled(boolean state) {
        }
    }

    static final class Abort extends CancellableStoppableEvent {
    }

    @Getter
    public static final class CountingListener {
        private int pings;
        private int adminPings;

        @EventTarget
        public void onPing(Ping event) {
            pings++;
        }

        @EventTarget
        public void onAdminPing(AdminPing event) {
            adminPings++;
        }
    }

    @Getter
    public static final class FieldListener {
        private final AtomicInteger calls = new AtomicInteger();

        @EventTarget
        private final EventListener<Ping> onPing = new EventListener<Ping>() {
            @Override
            public void onEvent(Ping event) {
                calls.incrementAndGet();
            }
        };
    }

    public static final class StaticHandlers {
        @Getter
        private static final AtomicInteger pings = new AtomicInteger();

        @EventTarget
        public static void onPing(Ping event) {
            pings.incrementAndGet();
        }
    }

    public static final class MoreStaticHandlers {
        @Getter
        private static final AtomicInteger pings = new AtomicInteger();

        @EventTarget
        public static void onPing(Ping event) {
            pings.incrementAndGet();
        }
    }

    @Getter
    @Setter
    public static final class ToggleListener implements EventSubscriber {
        private boolean enabled = true;
        private int pings;

        @EventTarget
        public void onPing(Ping event) {
            pings++;
        }

        @Override
        public boolean isHandlingEvents() {
            return enabled;
        }
    }

    @Getter
    public static class ParentListener {
        private int parentPings;

        @EventTarget
        public void onPing(Ping event) {
            parentPings++;
        }
    }

    @Getter
    public static final class ChildListener extends ParentListener {
        private int childPings;

        @Override
        @EventTarget
        public void onPing(Ping event) {
            childPings++;
        }
    }

    @Getter
    public static final class PrivateListener {
        private int pings;

        @EventTarget
        private void onPing(Ping event) {
            pings++;
        }
    }
}
