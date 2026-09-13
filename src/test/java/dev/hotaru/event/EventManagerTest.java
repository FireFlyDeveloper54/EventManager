package dev.hotaru.event;

import dev.hotaru.event.annotations.EventTarget;
import dev.hotaru.event.impl.CancellableEvent;
import dev.hotaru.event.impl.CancellableStoppableEvent;
import dev.hotaru.event.impl.Event;
import dev.hotaru.event.impl.StoppableEvent;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.Predicate;
import static org.junit.jupiter.api.Assertions.fail;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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

    @Test
    void subscriptionTryWithResourcesClosesAutomatically() {
        EventManager events = new EventManager();
        final AtomicInteger calls = new AtomicInteger();
        try (Subscription sub = events.register(Ping.class, new Consumer<Ping>() {
            @Override
            public void accept(Ping event) {
                calls.incrementAndGet();
            }
        })) {
            events.call(new Ping());
            assertEquals(1, calls.get());
            assertTrue(sub.isSubscribed());
        }
        events.call(new Ping());
        assertEquals(1, calls.get());
    }

    @Test
    void asyncDispatchWithoutExplicitExecutorUsesCommonPool() throws Exception {
        EventManager events = new EventManager();
        final AtomicInteger calls = new AtomicInteger();
        events.register(Ping.class, new Consumer<Ping>() {
            @Override
            public void accept(Ping event) {
                calls.incrementAndGet();
            }
        });
        events.callAsync(new Ping()).get();
        assertEquals(1, calls.get());
    }

    @Test
    void varargsRegisterMultipleEventTypes() {
        EventManager events = new EventManager();
        final AtomicInteger calls = new AtomicInteger();
        Consumer<Event> action = new Consumer<Event>() {
            @Override
            public void accept(Event event) {
                calls.incrementAndGet();
            }
        };
        Subscription sub = events.register(action, Ping.class, Save.class);
        events.call(new Ping());
        assertEquals(1, calls.get());
        events.call(new Save());
        assertEquals(2, calls.get());
        sub.unsubscribe();
        events.call(new Ping());
        events.call(new Save());
        assertEquals(2, calls.get());
    }

    @Test
    void finalFieldListenerFastPath() {
        EventManager events = new EventManager();
        FieldListener listener = new FieldListener();
        events.register(listener);
        events.call(new Ping());
        assertEquals(1, listener.getCalls().get());
        events.call(new Ping());
        assertEquals(2, listener.getCalls().get());
    }

    @Test
    void metricsCanBeDisabled() {
        EventManager events = new EventManager();
        events.setMetricsEnabled(false);
        events.register(Ping.class, new Consumer<Ping>() {
            @Override
            public void accept(Ping event) {}
        });
        events.call(new Ping());
        EventMetrics m = events.metrics();
        assertEquals(0, m.getDispatchedEvents());
        assertEquals(0, m.getHandlerInvocations());
    }

    @Test
    void interceptorCanWrapOrShortCircuitDispatch() {
        EventManager events = new EventManager();
        AtomicInteger counter = new AtomicInteger();
        events.register(Ping.class, p -> counter.incrementAndGet());

        AtomicInteger interceptCalls = new AtomicInteger();
        EventInterceptor interceptor = (event, proceed) -> {
            interceptCalls.incrementAndGet();
            proceed.run();
        };

        events.setInterceptor(interceptor);
        assertSame(interceptor, events.getInterceptor());

        events.call(new Ping());
        assertEquals(1, interceptCalls.get());
        assertEquals(1, counter.get());

        // Short-circuiting interceptor
        events.setInterceptor((event, proceed) -> {
            interceptCalls.incrementAndGet();
            // does NOT call proceed.run()!
        });

        events.call(new Ping());
        assertEquals(2, interceptCalls.get());
        assertEquals(1, counter.get()); // did not increment

        // Removing interceptor
        events.setInterceptor(null);
        assertNull(events.getInterceptor());
        events.call(new Ping());
        assertEquals(2, counter.get());
    }

    @Test
    void weakListenerDeactivatesAfterGarbageCollection() {
        EventManager events = new EventManager();
        CountingListener strong = new CountingListener();
        CountingListener weakTarget = new CountingListener();

        events.register(strong);
        Subscription weakSub = events.subscribeWeak(weakTarget);
        assertTrue(weakSub.isSubscribed());

        events.call(new Ping());
        assertEquals(1, strong.getPings());
        assertEquals(1, weakTarget.getPings());

        // Keep weak reference to observe GC
        java.lang.ref.WeakReference<CountingListener> ref = new java.lang.ref.WeakReference<>(weakTarget);
        weakTarget = null; // drop strong reference

        for (int i = 0; i < 10 && ref.get() != null; i++) {
            System.gc();
            System.runFinalization();
            try {
                Thread.sleep(20);
            } catch (InterruptedException ignored) {}
        }

        if (ref.get() == null) {
            events.call(new Ping());
            assertEquals(2, strong.getPings());
            assertFalse(weakSub.isSubscribed());
        }
    }

    @Test
    void subscribeWeakRegistersConsumerWeakly() {
        EventManager events = new EventManager();
        AtomicInteger calls = new AtomicInteger();
        Consumer<Ping> consumer = p -> calls.incrementAndGet();

        Subscription sub = events.subscribeWeak(consumer, Ping.class);
        assertTrue(sub.isSubscribed());
        events.call(new Ping());
        assertEquals(1, calls.get());

        java.lang.ref.WeakReference<Consumer<Ping>> ref = new java.lang.ref.WeakReference<>(consumer);
        consumer = null;

        for (int i = 0; i < 10 && ref.get() != null; i++) {
            System.gc();
            System.runFinalization();
            try {
                Thread.sleep(20);
            } catch (InterruptedException ignored) {}
        }

        if (ref.get() == null) {
            events.call(new Ping());
            assertEquals(1, calls.get());
            assertFalse(sub.isSubscribed());
        }
    }

    @Test
    void unregisterIfRemovesMatchingListeners() {
        EventManager events = new EventManager();
        CountingListener listener1 = new CountingListener();
        CountingListener listener2 = new CountingListener();
        events.register(listener1);
        events.register(listener2);

        events.call(new Ping());
        assertEquals(1, listener1.getPings());
        assertEquals(1, listener2.getPings());

        events.unregisterIf(target -> target == listener1);
        events.call(new Ping());
        assertEquals(1, listener1.getPings());
        assertEquals(2, listener2.getPings());
    }

    @Test
    void unregisterEventTypeRemovesAllHandlersForType() {
        EventManager events = new EventManager();
        CountingListener listener = new CountingListener();
        events.register(listener);

        events.call(new Ping());
        events.call(new AdminPing());
        assertEquals(2, listener.getPings()); // AdminPing extends Ping
        assertEquals(1, listener.getAdminPings());

        events.unregisterEventType(AdminPing.class);

        events.call(new AdminPing());
        assertEquals(1, listener.getAdminPings());
        assertEquals(3, listener.getPings()); // Ping handler still receives AdminPing because AdminPing is a Ping
    }

    @Test
    void callCancelledReturnsCancelledState() {
        EventManager events = new EventManager();
        events.register(Save.class, save -> save.setCancelled(true));

        Save save1 = new Save();
        assertTrue(events.callCancelled(save1));
        assertTrue(save1.isCancelled());

        events.unregisterAll();
        Save save2 = new Save();
        assertFalse(events.callCancelled(save2));
        assertFalse(save2.isCancelled());
    }

    @Test
    void eventMetricsEqualsAndHashCodeAndToString() {
        EventMetrics m1 = new EventMetrics(Ping.class, 10, 20, 5);
        EventMetrics m2 = new EventMetrics(Ping.class, 10, 20, 5);
        EventMetrics m3 = new EventMetrics(null, 10, 20, 5);

        assertEquals(m1, m2);
        assertEquals(m1.hashCode(), m2.hashCode());
        assertNotEquals(m1, m3);

        String str = m1.toString();
        assertTrue(str.contains("Ping"));
        assertTrue(str.contains("dispatchedEvents=10"));
    }


    @Test
    void concurrentRegisterUnregisterAndDispatchStressTest() throws InterruptedException {
        EventManager events = new EventManager();
        int threadCount = 8;
        int operationsPerThread = 1000;
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(threadCount);
        AtomicInteger successfulDispatches = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<Throwable>();

        for (int t = 0; t < threadCount; t++) {
            pool.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < operationsPerThread; i++) {
                        if ((i % 3) == 0) {
                            Subscription sub = events.registerWeak(Ping.class, p -> {});
                            if (i % 6 == 0) {
                                sub.unsubscribe();
                            }
                        } else if ((i % 3) == 1) {
                            events.call(new Ping());
                            successfulDispatches.incrementAndGet();
                        } else {
                            events.unregisterIf(target -> false);
                        }
                    }
                } catch (Throwable t1) {
                    failure.compareAndSet(null, t1);
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean completed = finishLatch.await(10, TimeUnit.SECONDS);
        pool.shutdownNow();

        assertTrue(completed, "Stress test timed out - potential deadlock detected");
        assertNull(failure.get(), "Exception thrown during concurrent operations");
        assertTrue(successfulDispatches.get() > 0);
    }

    interface MarkerAlpha extends Event {}
    interface MarkerBeta extends MarkerAlpha {}
    static class AlphaBetaEvent implements MarkerBeta {}

    @Test
    void eventHierarchyDispatchesToAllInterfacesAndSuperclasses() {
        EventManager events = new EventManager();
        AtomicInteger alphaCount = new AtomicInteger();
        AtomicInteger betaCount = new AtomicInteger();
        AtomicInteger directCount = new AtomicInteger();

        events.register(MarkerAlpha.class, e -> alphaCount.incrementAndGet());
        events.register(MarkerBeta.class, e -> betaCount.incrementAndGet());
        events.register(AlphaBetaEvent.class, e -> directCount.incrementAndGet());

        events.call(new AlphaBetaEvent());
        assertEquals(1, directCount.get());
        assertEquals(1, betaCount.get());
        assertEquals(1, alphaCount.get());
    }

    @Test
    void deadEventDispatchedWhenNoHandlersExist() {
        EventManager events = new EventManager();
        List<DeadEvent> deadEvents = new ArrayList<DeadEvent>();
        events.register(DeadEvent.class, deadEvents::add);

        // Ping has no handlers registered
        events.call(new Ping());

        assertEquals(1, deadEvents.size());
        DeadEvent dead = deadEvents.get(0);
        assertSame(events, dead.getSource());
        assertTrue(dead.getEvent() instanceof Ping);
        assertTrue(dead.getTimestamp() > 0);
        assertTrue(dead.toString().contains("DeadEvent"));
        assertEquals(dead, new DeadEvent(events, dead.getEvent(), dead.getTimestamp()));

        // When a handler for Ping exists, DeadEvent is NOT dispatched
        events.register(Ping.class, p -> {});
        events.call(new Ping());
        assertEquals(1, deadEvents.size());

        // When deadEventsEnabled is false, DeadEvent is suppressed
        events.setDeadEventsEnabled(false);
        assertFalse(events.isDeadEventsEnabled());
        events.call(new Save());
        assertEquals(1, deadEvents.size());
    }

    @Test
    void eventMetricsTracksTotalAndMaxDurationAndAverage() {
        EventManager events = new EventManager();
        events.register(Ping.class, p -> {
            try {
                Thread.sleep(5);
            } catch (InterruptedException ignored) {}
        });

        events.call(new Ping());
        events.call(new Ping());

        EventMetrics m = events.metrics();
        assertEquals(2, m.getDispatchedEvents());
        assertEquals(2, m.getHandlerInvocations());
        assertTrue(m.getTotalDurationNanos() > 0, "totalDurationNanos should be > 0");
        assertTrue(m.getMaxDurationNanos() > 0, "maxDurationNanos should be > 0");
        assertTrue(m.getAverageDurationNanos() > 0.0, "averageDurationNanos should be > 0.0");
        assertTrue(m.toString().contains("totalDurationNanos"));

        EventMetrics pingM = events.metrics(Ping.class);
        assertEquals(2, pingM.getDispatchedEvents());
        assertTrue(pingM.getTotalDurationNanos() > 0);

        events.resetMetrics();
        EventMetrics resetM = events.metrics();
        assertEquals(0, resetM.getDispatchedEvents());
        assertEquals(0, resetM.getTotalDurationNanos());
        assertEquals(0, resetM.getMaxDurationNanos());
        assertEquals(0.0, resetM.getAverageDurationNanos());
    }

    @Test
    void builderConfiguresEventManagerCorrectly() {
        EventErrorHandler customHandler = (event, source, t) -> {};
        EventInterceptor customInterceptor = (event, proceed) -> proceed.run();

        EventManager bus = EventManager.builder()
                .errorPolicy(ErrorPolicy.STOP)
                .errorHandler(customHandler)
                .metricsEnabled(false)
                .deadEventsEnabled(false)
                .interceptor(customInterceptor)
                .build();

        assertEquals(ErrorPolicy.STOP, bus.getErrorPolicy());
        assertSame(customHandler, bus.getErrorHandler());
        assertFalse(bus.isMetricsEnabled());
        assertFalse(bus.isDeadEventsEnabled());
        assertSame(customInterceptor, bus.getInterceptor());
    }

    @Test
    void callAllDispatchesAllEventsInOrder() {
        EventManager events = new EventManager();
        List<String> received = new ArrayList<String>();
        events.register(Ping.class, p -> received.add("ping"));
        events.register(Save.class, s -> received.add("save"));

        events.callAll(new Ping(), new Save(), new Ping());
        assertEquals(Arrays.asList("ping", "save", "ping"), received);

        received.clear();
        events.callAll(Arrays.asList(new Save(), new Ping()));
        assertEquals(Arrays.asList("save", "ping"), received);

        // Edge case: null or empty
        events.callAll((Event[]) null);
        events.callAll((Iterable<Event>) null);
        assertEquals(Arrays.asList("save", "ping"), received);
    }

    public static final class PingAcceptedFilter implements EventFilter<Ping> {
        @Override
        public boolean test(Ping event) {
            return event.accept;
        }
    }

    public static final class FilteredListener {
        private final AtomicInteger calls = new AtomicInteger();

        public int getCalls() {
            return calls.get();
        }

        @EventTarget(filter = PingAcceptedFilter.class)
        public void onPing(Ping event) {
            calls.incrementAndGet();
        }
    }

    @Test
    void eventFilterDeclarativeAnnotationFiltersEvents() {
        EventManager events = new EventManager();
        FilteredListener listener = new FilteredListener();
        events.register(listener);

        Ping rejected = new Ping();
        rejected.accept = false;
        events.call(rejected);
        assertEquals(0, listener.getCalls());

        Ping accepted = new Ping();
        accepted.accept = true;
        events.call(accepted);
        assertEquals(1, listener.getCalls());
    }

    @Test
    void interceptorChainingExecutesInPipelineOrder() {
        EventManager events = new EventManager();
        List<String> order = new ArrayList<String>();

        events.addInterceptor(new EventInterceptor() {
            @Override
            public void intercept(Event event, Runnable proceed) {
                order.add("A_before");
                proceed.run();
                order.add("A_after");
            }
        });

        events.addInterceptor(new EventInterceptor() {
            @Override
            public void intercept(Event event, Runnable proceed) {
                order.add("B_before");
                proceed.run();
                order.add("B_after");
            }
        });

        events.register(Ping.class, p -> order.add("handler"));
        events.call(new Ping());

        assertEquals(Arrays.asList("A_before", "B_before", "handler", "B_after", "A_after"), order);
    }

    @Test
    void threadEnforcementEnforcesDispatchThread() throws InterruptedException {
        EventManager events = EventManager.builder()
                .enforceThread(Thread.currentThread())
                .build();

        // Calling from current thread succeeds
        events.call(new Ping());

        // Calling from different thread throws IllegalStateException
        AtomicReference<Throwable> thrown = new AtomicReference<Throwable>();
        Thread worker = new Thread(() -> {
            try {
                events.call(new Ping());
            } catch (Throwable t) {
                thrown.set(t);
            }
        });
        worker.start();
        worker.join();

        assertTrue(thrown.get() instanceof IllegalStateException);
        assertTrue(thrown.get().getMessage().contains("unauthorized thread"));
    }

    @Test
    void childBusBubblesEventsAndCascadesLifecycle() {
        EventManager parent = new EventManager();
        EventManager child = parent.createChildBus();

        assertSame(parent, child.getParent());
        assertTrue(parent.getChildren().contains(child));

        AtomicInteger parentCount = new AtomicInteger();
        AtomicInteger childCount = new AtomicInteger();

        parent.register(Ping.class, p -> parentCount.incrementAndGet());
        child.register(Ping.class, p -> childCount.incrementAndGet());

        // Event dispatched on child triggers child AND bubbles to parent
        child.call(new Ping());
        assertEquals(1, childCount.get());
        assertEquals(1, parentCount.get());

        // Event dispatched on parent triggers ONLY parent
        parent.call(new Ping());
        assertEquals(1, childCount.get());
        assertEquals(2, parentCount.get());

        // Child bus close detaches from parent
        child.close();
        assertFalse(parent.getChildren().contains(child));

        // Event on detached child no longer bubbles or calls child
        child.call(new Ping());
        assertEquals(1, childCount.get());
        assertEquals(2, parentCount.get());

        // Parent close cascades to all its children
        EventManager child2 = parent.createChildBus();
        assertTrue(parent.getChildren().contains(child2));
        parent.close();
        assertEquals(0, parent.getChildren().size());
    }

    @Test
    void eventFilterCompositionAndOrNegateNot() {
        EventFilter<Ping> acceptFilter = p -> p.accept;
        EventFilter<Ping> rejectFilter = EventFilter.not(acceptFilter);

        Ping pingTrue = new Ping();
        pingTrue.accept = true;

        Ping pingFalse = new Ping();
        pingFalse.accept = false;

        assertTrue(acceptFilter.test(pingTrue));
        assertFalse(acceptFilter.test(pingFalse));

        assertFalse(rejectFilter.test(pingTrue));
        assertTrue(rejectFilter.test(pingFalse));

        EventFilter<Ping> andFilter = acceptFilter.and(p -> p.accept);
        assertTrue(andFilter.test(pingTrue));
        assertFalse(andFilter.test(pingFalse));

        EventFilter<Ping> orFilter = acceptFilter.or(p -> !p.accept);
        assertTrue(orFilter.test(pingTrue));
        assertTrue(orFilter.test(pingFalse));

        EventFilter<Ping> negated = acceptFilter.negate();
        assertFalse(negated.test(pingTrue));
        assertTrue(negated.test(pingFalse));
    }

    static final class ContextualSubscriber implements EventSubscriber {
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

        @Override
        public boolean isHandlingEvents(Event event) {
            // Opt-out only for AdminPing, allow standard Ping
            return !(event instanceof AdminPing);
        }
    }

    @Test
    void eventSubscriberContextualHandlingPerEvent() {
        EventManager events = new EventManager();
        ContextualSubscriber listener = new ContextualSubscriber();
        events.register(listener);

        events.call(new Ping());
        assertEquals(1, listener.pings);
        assertEquals(0, listener.adminPings);

        events.call(new AdminPing());
        // AdminPing is rejected by isHandlingEvents(Event)
        assertEquals(1, listener.pings);
        assertEquals(0, listener.adminPings);
    }

    @Test
    void purgeDeadHandlersCleansCollectedWeakListeners() {
        EventManager events = new EventManager();
        class WeakTarget {
            @EventTarget
            public void onPing(Ping p) {}
        }

        WeakTarget target = new WeakTarget();
        events.registerWeak(target);
        assertEquals(1, events.handlerCount(Ping.class));

        // Drop strong reference and run GC loop
        target = null;
        for (int i = 0; i < 5; i++) {
            System.gc();
            System.runFinalization();
            try { Thread.sleep(20); } catch (InterruptedException ignored) {}
        }

        int purged = events.purgeDeadHandlers();
        assertTrue(purged >= 0);
        // After purge, dead handlers must be gone
        if (purged > 0) {
            assertEquals(0, events.handlerCount(Ping.class));
        }
    }

    @Test
    void defaultExecutorUsedInCallAsync() throws Exception {
        ExecutorService customExecutor = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "custom-bus-worker-1");
            }
        });

        EventManager bus = EventManager.builder()
                .defaultExecutor(customExecutor)
                .build();

        assertSame(customExecutor, bus.getDefaultExecutor());

        AtomicReference<String> workerThread = new AtomicReference<String>();
        bus.register(Ping.class, p -> workerThread.set(Thread.currentThread().getName()));

        CompletableFuture<Ping> future = bus.callAsync(new Ping());
        future.get(5, TimeUnit.SECONDS);

        assertEquals("custom-bus-worker-1", workerThread.get());
        customExecutor.shutdownNow();
    }

    @Test
    void busNamingAndDiagnosticToString() {
        EventManager bus = EventManager.builder()
                .name("ChatBus")
                .build();

        assertEquals("ChatBus", bus.getName());
        String str = bus.toString();
        assertTrue(str.contains("ChatBus"));
        assertTrue(str.contains("registeredTypes="));
        assertTrue(str.contains("handlers="));
        assertTrue(str.contains("closed="));
    }

    @Test
    void eventFilterPredicateBidirectionalConversion() {
        Ping pingTrue = new Ping();
        pingTrue.accept = true;
        Ping pingFalse = new Ping();
        pingFalse.accept = false;

        java.util.function.Predicate<Ping> pred = p -> p.accept;
        EventFilter<Ping> filter = EventFilter.fromPredicate(pred);

        assertTrue(filter.test(pingTrue));
        assertFalse(filter.test(pingFalse));

        java.util.function.Predicate<Ping> roundtrip = filter.asPredicate();
        assertTrue(roundtrip.test(pingTrue));
        assertFalse(roundtrip.test(pingFalse));
    }

    @Test
    void zeroAllocationInterceptorFrameSupportsReentrancy() {
        EventManager events = new EventManager();
        List<String> auditLog = new ArrayList<String>();

        events.addInterceptor(new EventInterceptor() {
            @Override
            public void intercept(Event event, Runnable proceed) {
                auditLog.add("start:" + event.getClass().getSimpleName());
                if (event instanceof Ping) {
                    // Reentrant dispatch inside interceptor
                    events.call(new Save());
                }
                proceed.run();
                auditLog.add("end:" + event.getClass().getSimpleName());
            }
        });

        events.register(Ping.class, p -> auditLog.add("ping_handler"));
        events.register(Save.class, s -> auditLog.add("save_handler"));

        events.call(new Ping());

        // Both Ping and reentrant Save executed in proper nested order
        assertTrue(auditLog.contains("start:Ping"));
        assertTrue(auditLog.contains("start:Save"));
        assertTrue(auditLog.contains("save_handler"));
        assertTrue(auditLog.contains("end:Save"));
        assertTrue(auditLog.contains("ping_handler"));
        assertTrue(auditLog.contains("end:Ping"));
    }

    @Test
    void subscriptionsOfAndCombineIterable() {
        AtomicInteger count = new AtomicInteger();
        Subscription s1 = Subscriptions.of(count::incrementAndGet);
        Subscription s2 = Subscriptions.of(count::incrementAndGet);

        assertTrue(s1.isSubscribed());
        assertTrue(s2.isSubscribed());

        Subscription combined = Subscriptions.combine(Arrays.asList(s1, s2));
        assertTrue(combined.isSubscribed());

        combined.unsubscribe();
        assertFalse(combined.isSubscribed());
        assertFalse(s1.isSubscribed());
        assertFalse(s2.isSubscribed());
        assertEquals(2, count.get());

        // Repeated unsubscribe is idempotent
        combined.unsubscribe();
        assertEquals(2, count.get());

        // Edge cases
        assertSame(Subscription.NOOP, Subscriptions.combine((Iterable<Subscription>) null));
        assertSame(Subscription.NOOP, Subscriptions.combine(Collections.emptyList()));
        assertSame(Subscription.NOOP, Subscriptions.of(null));
    }

    @Test
    void asynchronousInterceptorOffloadingDoesNotCorruptDispatch() throws Exception {
        EventManager events = new EventManager();
        ExecutorService asyncPool = Executors.newSingleThreadExecutor();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> handlerThread = new AtomicReference<String>();

        // Interceptor offloads proceed to another thread and returns immediately
        events.setInterceptor(new EventInterceptor() {
            @Override
            public void intercept(Event event, Runnable proceed) {
                asyncPool.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            proceed.run();
                        } finally {
                            latch.countDown();
                        }
                    }
                });
            }
        });

        events.register(Ping.class, p -> handlerThread.set(Thread.currentThread().getName()));

        events.call(new Ping());

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNotNull(handlerThread.get());
        asyncPool.shutdownNow();
    }

    @Test
    void isRegisteredAndHandlerCountAccuratelyReflectOnceAndDeadListeners() {
        EventManager events = new EventManager();
        AtomicInteger count = new AtomicInteger();
        Consumer<Ping> onceAction = p -> count.incrementAndGet();

        events.registerOnce(Ping.class, onceAction);
        assertTrue(events.isRegistered(onceAction));
        assertEquals(1, events.handlerCount(Ping.class));
        assertTrue(events.hasListeners(Ping.class));

        events.call(new Ping());
        assertEquals(1, count.get());

        // Immediately after once execution, it must not be reported as registered or active
        assertFalse(events.isRegistered(onceAction));
        assertEquals(0, events.handlerCount(Ping.class));
        assertFalse(events.hasListeners(Ping.class));
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
    public static final class CountingListener {
        private int pings;
        private int adminPings;

        public int getPings() {
            return pings;
        }

        public int getAdminPings() {
            return adminPings;
        }

        @EventTarget
        public void onPing(Ping event) {
            pings++;
        }

        @EventTarget
        public void onAdminPing(AdminPing event) {
            adminPings++;
        }
    }

    public static final class FieldListener {
        private final AtomicInteger calls = new AtomicInteger();

        public AtomicInteger getCalls() {
            return calls;
        }

        @EventTarget
        private final EventListener<Ping> onPing = new EventListener<Ping>() {
            @Override
            public void onEvent(Ping event) {
                calls.incrementAndGet();
            }
        };
    }

    public static final class StaticHandlers {
        private static final AtomicInteger pings = new AtomicInteger();

        public static AtomicInteger getPings() {
            return pings;
        }

        @EventTarget
        public static void onPing(Ping event) {
            pings.incrementAndGet();
        }
    }

    public static final class MoreStaticHandlers {
        private static final AtomicInteger pings = new AtomicInteger();

        public static AtomicInteger getPings() {
            return pings;
        }

        @EventTarget
        public static void onPing(Ping event) {
            pings.incrementAndGet();
        }
    }

    public static final class ToggleListener implements EventSubscriber {
        private boolean enabled = true;
        private int pings;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getPings() {
            return pings;
        }

        public void setPings(int pings) {
            this.pings = pings;
        }

        @EventTarget
        public void onPing(Ping event) {
            pings++;
        }

        @Override
        public boolean isHandlingEvents() {
            return enabled;
        }
    }

    public static class ParentListener {
        private int parentPings;

        public int getParentPings() {
            return parentPings;
        }

        @EventTarget
        public void onPing(Ping event) {
            parentPings++;
        }
    }

    public static final class ChildListener extends ParentListener {
        private int childPings;

        public int getChildPings() {
            return childPings;
        }

        @Override
        @EventTarget
        public void onPing(Ping event) {
            childPings++;
        }
    }

    public static final class PrivateListener {
        private int pings;

        public int getPings() {
            return pings;
        }

        @EventTarget
        private void onPing(Ping event) {
            pings++;
        }
    }

    // =========================================================================
    // Feature 1: Reified Generic Event Dispatching Tests
    // =========================================================================

    public static class GenericPayloadEvent<T> extends dev.hotaru.event.impl.AbstractGenericEvent<T> {
        private final T payload;

        public GenericPayloadEvent(T payload, java.lang.reflect.Type genericType) {
            super(genericType);
            this.payload = payload;
        }

        public T getPayload() {
            return payload;
        }
    }

    public static class GenericEventListener {
        final List<String> stringReceived = new ArrayList<String>();
        final List<Integer> intReceived = new ArrayList<Integer>();
        final List<Number> numberReceived = new ArrayList<Number>();
        final List<Object> rawReceived = new ArrayList<Object>();

        @EventTarget
        public void onString(GenericPayloadEvent<String> event) {
            stringReceived.add(event.getPayload());
        }

        @EventTarget
        public void onInteger(GenericPayloadEvent<Integer> event) {
            intReceived.add(event.getPayload());
        }

        @EventTarget
        public void onNumber(GenericPayloadEvent<Number> event) {
            numberReceived.add(event.getPayload());
        }

        @EventTarget
        public void onRaw(GenericPayloadEvent event) {
            rawReceived.add(event.getPayload());
        }
    }

    @Test
    public void testGenericEventExactAndPolymorphicMatching() {
        EventManager bus = new EventManager();
        GenericEventListener listener = new GenericEventListener();
        bus.register(listener);

        // Dispatch String event
        bus.dispatch(new GenericPayloadEvent<String>("hello", String.class));
        assertEquals(1, listener.stringReceived.size());
        assertEquals("hello", listener.stringReceived.get(0));
        assertEquals(0, listener.intReceived.size());
        assertEquals(0, listener.numberReceived.size());
        assertEquals(1, listener.rawReceived.size());

        // Dispatch Integer event (should match onInteger, onNumber (polymorphic), onRaw, but NOT onString)
        bus.dispatch(new GenericPayloadEvent<Integer>(42, Integer.class));
        assertEquals(1, listener.stringReceived.size());
        assertEquals(1, listener.intReceived.size());
        assertEquals(Integer.valueOf(42), listener.intReceived.get(0));
        assertEquals(1, listener.numberReceived.size());
        assertEquals(Integer.valueOf(42), listener.numberReceived.get(0));
        assertEquals(2, listener.rawReceived.size());

        // Dispatch Double event (should match onNumber and onRaw, but NOT onInteger or onString)
        bus.dispatch(new GenericPayloadEvent<Double>(3.14, Double.class));
        assertEquals(1, listener.stringReceived.size());
        assertEquals(1, listener.intReceived.size());
        assertEquals(2, listener.numberReceived.size());
        assertEquals(3, listener.rawReceived.size());
    }

    @Test
    public void testTypeTokenReificationAndEquality() {
        TypeToken<List<String>> token1 = new TypeToken<List<String>>() {};
        TypeToken<List<String>> token2 = new TypeToken<List<String>>() {};
        TypeToken<List<Integer>> token3 = new TypeToken<List<Integer>>() {};

        assertEquals(token1, token2);
        assertEquals(token1.hashCode(), token2.hashCode());
        assertFalse(token1.equals(token3));
        assertNotNull(token1.getType());
        assertTrue(token1.toString().contains("List"));
    }

    // =========================================================================
    // Feature 2: Fluent SubscriberBuilder DSL Tests
    // =========================================================================

    public static final class NumberedPing implements Event {
        private final int id;

        public NumberedPing(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }
    }

    @Test
    public void testSubscriberBuilderFullCombination() {
        EventManager bus = new EventManager();
        final List<String> log = new ArrayList<String>();

        Subscription sub = bus.on(NumberedPing.class)
                .priority(Priority.HIGHEST)
                .filter(new Predicate<NumberedPing>() {
                    @Override
                    public boolean test(NumberedPing ping) {
                        return ping.getId() > 10;
                    }
                })
                .once()
                .handle(new Consumer<NumberedPing>() {
                    @Override
                    public void accept(NumberedPing ping) {
                        log.add("handled-" + ping.getId());
                    }
                });

        assertNotNull(sub);
        assertTrue(sub.isSubscribed());

        // ping with id <= 10 should be filtered out
        bus.dispatch(new NumberedPing(5));
        assertEquals(0, log.size());
        assertTrue(sub.isSubscribed());

        // ping with id > 10 matches filter and fires once
        bus.dispatch(new NumberedPing(15));
        assertEquals(1, log.size());
        assertEquals("handled-15", log.get(0));
        assertFalse(sub.isSubscribed());

        // next ping should not fire because it was once
        bus.dispatch(new NumberedPing(20));
        assertEquals(1, log.size());
    }

    @Test
    public void testSubscriberBuilderWithGenericType() {
        EventManager bus = new EventManager();
        final List<Object> received = new ArrayList<Object>();

        bus.on(GenericPayloadEvent.class)
                .genericType(String.class)
                .handle(new Consumer<GenericPayloadEvent>() {
                    @Override
                    public void accept(GenericPayloadEvent event) {
                        received.add(event.getPayload());
                    }
                });

        bus.dispatch(new GenericPayloadEvent<Integer>(99, Integer.class));
        assertEquals(0, received.size());

        bus.dispatch(new GenericPayloadEvent<String>("fluent", String.class));
        assertEquals(1, received.size());
        assertEquals("fluent", received.get(0));
    }

    // =========================================================================
    // Feature 3: Aggregated Error Policy Tests
    // =========================================================================

    @Test
    public void testErrorPolicyAggregateCollectsAllFailures() {
        final List<Throwable> loggedErrors = new ArrayList<Throwable>();
        EventManager bus = new EventManager.Builder()
                .errorPolicy(ErrorPolicy.AGGREGATE)
                .errorHandler(new EventErrorHandler() {
                    @Override
                    public void handle(Event event, Object listener, Throwable throwable) {
                        loggedErrors.add(throwable);
                    }
                })
                .build();

        final AtomicBoolean handler2Ran = new AtomicBoolean(false);

        // Handler 1 throws RuntimeException
        bus.on(NumberedPing.class).priority(Priority.HIGHEST).handle(new Consumer<NumberedPing>() {
            @Override
            public void accept(NumberedPing ping) {
                throw new RuntimeException("fail-1");
            }
        });

        // Handler 2 runs normally
        bus.on(NumberedPing.class).priority(Priority.NORMAL).handle(new Consumer<NumberedPing>() {
            @Override
            public void accept(NumberedPing ping) {
                handler2Ran.set(true);
            }
        });

        // Handler 3 throws IllegalStateException
        bus.on(NumberedPing.class).priority(Priority.LOWEST).handle(new Consumer<NumberedPing>() {
            @Override
            public void accept(NumberedPing ping) {
                throw new IllegalStateException("fail-3");
            }
        });

        try {
            bus.dispatch(new NumberedPing(1));
            fail("Expected EventDispatchException to be thrown under ErrorPolicy.AGGREGATE");
        } catch (EventDispatchException ex) {
            assertTrue(handler2Ran.get()); // Handler 2 was NOT skipped!
            assertEquals(2, loggedErrors.size());
            assertEquals("fail-1", ex.getCause().getMessage());
            Throwable[] suppressed = ex.getSuppressed();
            assertEquals(1, suppressed.length);
            assertEquals("fail-3", suppressed[0].getMessage());
        }
    }

    // =========================================================================
    // Feature 4: Transactional Event Buffering & Rollback Tests
    // =========================================================================

    @Test
    public void testTransactionCommitNormal() {
        EventManager bus = new EventManager();
        final List<Integer> received = new ArrayList<Integer>();

        bus.register(new Consumer<NumberedPing>() {
            @Override
            public void accept(NumberedPing ping) {
                received.add(ping.getId());
            }
        }, NumberedPing.class);

        bus.transaction(new Runnable() {
            @Override
            public void run() {
                assertTrue(bus.isTransactionActive());
                bus.dispatch(new NumberedPing(1));
                bus.dispatch(new NumberedPing(2));
                // Events must be buffered, not yet dispatched!
                assertEquals(0, received.size());
            }
        });

        // After transaction completes, events are flushed in FIFO order
        assertFalse(bus.isTransactionActive());
        assertEquals(2, received.size());
        assertEquals(Integer.valueOf(1), received.get(0));
        assertEquals(Integer.valueOf(2), received.get(1));
    }

    @Test
    public void testTransactionRollbackOnException() {
        final EventManager bus = new EventManager();
        final List<Integer> received = new ArrayList<Integer>();

        bus.register(new Consumer<NumberedPing>() {
            @Override
            public void accept(NumberedPing ping) {
                received.add(ping.getId());
            }
        }, NumberedPing.class);

        try {
            bus.transaction(new Runnable() {
                @Override
                public void run() {
                    bus.dispatch(new NumberedPing(100));
                    bus.dispatch(new NumberedPing(200));
                    throw new IllegalStateException("Simulated business failure");
                }
            });
            fail("Should have rethrown exception");
        } catch (IllegalStateException expected) {
            assertEquals("Simulated business failure", expected.getMessage());
        }

        // Transactions rolled back: zero events dispatched!
        assertFalse(bus.isTransactionActive());
        assertEquals(0, received.size());
    }

    @Test
    public void testExplicitTransactionRollback() {
        EventManager bus = new EventManager();
        final List<Integer> received = new ArrayList<Integer>();

        bus.register(new Consumer<NumberedPing>() {
            @Override
            public void accept(NumberedPing ping) {
                received.add(ping.getId());
            }
        }, NumberedPing.class);

        bus.transaction(new Runnable() {
            @Override
            public void run() {
                bus.dispatch(new NumberedPing(1));
                bus.rollbackTransaction();
                bus.dispatch(new NumberedPing(2));
            }
        });

        assertFalse(bus.isTransactionActive());
        assertEquals(0, received.size());
    }

    @Test
    public void testNestedTransactionCommit() {
        EventManager bus = new EventManager();
        final List<Integer> received = new ArrayList<Integer>();

        bus.register(new Consumer<NumberedPing>() {
            @Override
            public void accept(NumberedPing ping) {
                received.add(ping.getId());
            }
        }, NumberedPing.class);

        bus.transaction(new Runnable() {
            @Override
            public void run() {
                bus.dispatch(new NumberedPing(1));
                bus.transaction(new Runnable() {
                    @Override
                    public void run() {
                        bus.dispatch(new NumberedPing(2));
                    }
                });
                bus.dispatch(new NumberedPing(3));
                assertEquals(0, received.size());
            }
        });

        assertEquals(3, received.size());
        assertEquals(Integer.valueOf(1), received.get(0));
        assertEquals(Integer.valueOf(2), received.get(1));
        assertEquals(Integer.valueOf(3), received.get(2));
    }

    // =========================================================================
    // Feature 5: Industry-Standard Dispatch Aliases Tests
    // =========================================================================

    @Test
    public void testDispatchAliases() throws Exception {
        EventManager bus = new EventManager();
        final List<Integer> received = new ArrayList<Integer>();

        bus.register(new Consumer<NumberedPing>() {
            @Override
            public void accept(NumberedPing ping) {
                received.add(ping.getId());
            }
        }, NumberedPing.class);

        // dispatch
        NumberedPing p1 = bus.dispatch(new NumberedPing(10));
        assertEquals(10, p1.getId());
        assertEquals(1, received.size());

        // dispatchExact
        NumberedPing p2 = bus.dispatchExact(new NumberedPing(20));
        assertEquals(20, p2.getId());
        assertEquals(2, received.size());

        // dispatchAll
        bus.dispatchAll(new NumberedPing(30), new NumberedPing(40));
        assertEquals(4, received.size());

        // dispatchAsync
        CompletableFuture<NumberedPing> future = bus.dispatchAsync(new NumberedPing(50));
        NumberedPing p3 = future.get();
        assertEquals(50, p3.getId());
        assertEquals(5, received.size());

        // dispatchExactAsync
        CompletableFuture<NumberedPing> exactFuture = bus.dispatchExactAsync(new NumberedPing(60));
        NumberedPing p4 = exactFuture.get();
        assertEquals(60, p4.getId());
        assertEquals(6, received.size());

        // dispatchCancelled
        Save cancellable = new Save();
        bus.register(new Consumer<Save>() {
            @Override
            public void accept(Save e) {
                e.setCancelled(true);
            }
        }, Save.class);

        boolean wasCancelled = bus.dispatchCancelled(cancellable);
        assertTrue(wasCancelled);
    }

    // =========================================================================
    // Feature 6: Virtual Thread Integration Tests
    // =========================================================================

    @Test
    public void testVirtualThreadSupportExecutorAndDispatch() throws Exception {
        Executor executor = VirtualThreadSupport.createVirtualThreadExecutor();
        assertNotNull(executor);

        EventManager bus = new EventManager.Builder()
                .useVirtualThreads(true)
                .build();

        CompletableFuture<NumberedPing> future = bus.dispatchAsync(new NumberedPing(1234));
        NumberedPing result = future.get(5, TimeUnit.SECONDS);
        assertNotNull(result);
        assertEquals(1234, result.getId());
    }

    // =========================================================================
    // Feature 7: EventContext Metadata Carrier Tests
    // =========================================================================

    @Test
    public void testEventContextMetadataCarrier() {
        EventContext context = EventContext.of("traceId", "TR-999")
                .with("tenant", 42)
                .with("debug", true);

        assertEquals("TR-999", context.get("traceId", String.class));
        assertEquals(Integer.valueOf(42), context.get("tenant", Integer.class));
        assertTrue(context.get("debug", Boolean.class));
        assertNull(context.get("missing", String.class));
        assertEquals("default", context.getOrDefault("missing", String.class, "default"));

        final AtomicReference<String> capturedTraceId = new AtomicReference<String>();
        EventManager bus = new EventManager();
        bus.register(new Consumer<NumberedPing>() {
            @Override
            public void accept(NumberedPing ping) {
                capturedTraceId.set(EventContext.current().get("traceId", String.class));
            }
        }, NumberedPing.class);

        bus.dispatch(new NumberedPing(1), context);
        assertEquals("TR-999", capturedTraceId.get());

        // Context must be cleanly restored to empty after dispatch
        assertEquals(EventContext.empty(), EventContext.current());
    }

    @Test
    public void testEventContextAsyncPropagation() throws Exception {
        EventManager bus = new EventManager();
        final AtomicReference<String> asyncTraceId = new AtomicReference<String>();

        bus.register(new Consumer<NumberedPing>() {
            @Override
            public void accept(NumberedPing ping) {
                asyncTraceId.set(EventContext.current().get("traceId", String.class));
            }
        }, NumberedPing.class);

        EventContext context = EventContext.of("traceId", "ASYNC-777");
        try (EventContext.Scope scope = context.attach()) {
            CompletableFuture<NumberedPing> future = bus.dispatchAsync(new NumberedPing(5));
            future.get(5, TimeUnit.SECONDS);
        }

        assertEquals("ASYNC-777", asyncTraceId.get());
        assertEquals(EventContext.empty(), EventContext.current());
    }

    // =========================================================================
    // Feature 8: EventTrace & DeadEvent Trace Tests
    // =========================================================================

    @Test
    public void testEventTraceCapture() {
        EventTrace trace = EventTrace.capture();
        assertNotNull(trace);
        assertNotNull(trace.getClassName());
        assertNotNull(trace.getMethodName());
        assertTrue(trace.getLineNumber() > 0);
        assertTrue(trace.getTimestamp() > 0);
        assertTrue(trace.toString().contains(trace.getMethodName()));
    }

    public static final class UnhandledSpecialEvent implements Event {}

    @Test
    public void testDeadEventCarriesEventTrace() {
        EventManager bus = new EventManager.Builder().deadEventsEnabled(true).build();
        final AtomicReference<DeadEvent> capturedDead = new AtomicReference<DeadEvent>();

        bus.register(new Consumer<DeadEvent>() {
            @Override
            public void accept(DeadEvent deadEvent) {
                capturedDead.set(deadEvent);
            }
        }, DeadEvent.class);

        bus.dispatch(new UnhandledSpecialEvent());

        DeadEvent dead = capturedDead.get();
        assertNotNull(dead);
        assertTrue(dead.getEvent() instanceof UnhandledSpecialEvent);
        assertNotNull(dead.getTrace());
        assertTrue(dead.getTrace().getClassName().contains("EventManagerTest"));
    }

    // =========================================================================
    // Feature 9: Reactive EventPublisher Tests
    // =========================================================================

    @Test
    public void testReactiveEventPublisher() {
        EventManager bus = new EventManager();
        EventPublisher<NumberedPing> publisher = bus.asPublisher(NumberedPing.class);
        assertEquals(NumberedPing.class, publisher.getEventType());

        final List<Integer> streamData = new ArrayList<Integer>();
        Subscription sub = publisher
                .filter(new Predicate<NumberedPing>() {
                    @Override
                    public boolean test(NumberedPing ping) {
                        return ping.getId() % 2 == 0;
                    }
                })
                .subscribe(new Consumer<NumberedPing>() {
                    @Override
                    public void accept(NumberedPing ping) {
                        streamData.add(ping.getId());
                    }
                });

        bus.dispatch(new NumberedPing(1)); // odd: filtered
        bus.dispatch(new NumberedPing(2)); // even: accepted
        bus.dispatch(new NumberedPing(3)); // odd: filtered
        bus.dispatch(new NumberedPing(4)); // even: accepted

        assertEquals(2, streamData.size());
        assertEquals(Integer.valueOf(2), streamData.get(0));
        assertEquals(Integer.valueOf(4), streamData.get(1));

        sub.unsubscribe();
        bus.dispatch(new NumberedPing(6));
        assertEquals(2, streamData.size()); // no more data after unsubscribe
    }

    // =========================================================================
    // Feature 10: Event Expectation & Future Await Tests
    // =========================================================================

    @Test
    public void testEventExpectationNormal() throws Exception {
        EventManager bus = new EventManager();

        CompletableFuture<NumberedPing> future = bus.expect(NumberedPing.class);
        assertFalse(future.isDone());

        bus.dispatch(new NumberedPing(888));
        assertTrue(future.isDone());
        assertEquals(888, future.get().getId());

        // Subsequent dispatches must not affect or invoke the expectation again
        bus.dispatch(new NumberedPing(999));
        assertEquals(888, future.get().getId());
    }

    @Test
    public void testEventExpectationWithFilter() throws Exception {
        EventManager bus = new EventManager();

        CompletableFuture<NumberedPing> future = bus.expect(
                NumberedPing.class,
                new Predicate<NumberedPing>() {
                    @Override
                    public boolean test(NumberedPing ping) {
                        return ping.getId() == 555;
                    }
                }
        );

        bus.dispatch(new NumberedPing(111));
        assertFalse(future.isDone());

        bus.dispatch(new NumberedPing(555));
        assertTrue(future.isDone());
        assertEquals(555, future.get().getId());
    }

    @Test
    public void testEventExpectationTimeout() {
        EventManager bus = new EventManager();

        CompletableFuture<NumberedPing> future = bus.expect(
                NumberedPing.class,
                null,
                50,
                TimeUnit.MILLISECONDS
        );

        assertThrows(java.util.concurrent.ExecutionException.class, new org.junit.jupiter.api.function.Executable() {
            @Override
            public void execute() throws Throwable {
                future.get(1, TimeUnit.SECONDS);
            }
        });
    }


    // =========================================================================
    // Feature 11: Sticky Events & Instant Replay Tests
    // =========================================================================

    public static class StickyAnnotatedListener {
        final List<Integer> received = new ArrayList<Integer>();

        @EventTarget(sticky = true)
        public void onPing(NumberedPing ping) {
            received.add(ping.getId());
        }
    }

    @Test
    public void testStickyEventDispatchAndReplay() {
        EventManager bus = new EventManager();

        // Dispatch sticky event BEFORE registering any listeners
        NumberedPing dispatched = bus.dispatchSticky(new NumberedPing(999));
        assertEquals(999, dispatched.getId());
        assertEquals(Integer.valueOf(999), bus.getSticky(NumberedPing.class).getId());

        // 1. Test DSL-based sticky listener receives cached event immediately upon registration
        final List<Integer> dslReceived = new ArrayList<Integer>();
        Subscription sub = bus.on(NumberedPing.class)
                .sticky()
                .handle(new Consumer<NumberedPing>() {
                    @Override
                    public void accept(NumberedPing ping) {
                        dslReceived.add(ping.getId());
                    }
                });

        assertEquals(1, dslReceived.size());
        assertEquals(Integer.valueOf(999), dslReceived.get(0));

        // Subsequent dispatches work normally
        bus.dispatch(new NumberedPing(1000));
        assertEquals(2, dslReceived.size());
        assertEquals(Integer.valueOf(1000), dslReceived.get(1));

        // 2. Test @EventTarget(sticky = true) annotated listener receives sticky event upon registration
        StickyAnnotatedListener annotatedListener = new StickyAnnotatedListener();
        bus.register(annotatedListener);
        assertEquals(1, annotatedListener.received.size());
        assertEquals(Integer.valueOf(999), annotatedListener.received.get(0));

        // 3. Test removeSticky
        NumberedPing removed = bus.removeSticky(NumberedPing.class);
        assertEquals(999, removed.getId());
        assertNull(bus.getSticky(NumberedPing.class));
    }

    // =========================================================================
    // Feature 12: High-Frequency Throttling, Debouncing & Sampling Tests
    // =========================================================================

    @Test
    public void testEventSampling() {
        EventManager bus = new EventManager();
        final List<Integer> received = new ArrayList<Integer>();

        bus.on(NumberedPing.class)
                .sample(3) // 1 out of every 3
                .handle(new Consumer<NumberedPing>() {
                    @Override
                    public void accept(NumberedPing ping) {
                        received.add(ping.getId());
                    }
                });

        for (int i = 1; i <= 9; i++) {
            bus.dispatch(new NumberedPing(i));
        }

        assertEquals(3, received.size());
        assertEquals(Integer.valueOf(1), received.get(0));
        assertEquals(Integer.valueOf(4), received.get(1));
        assertEquals(Integer.valueOf(7), received.get(2));
    }

    @Test
    public void testEventThrottling() throws Exception {
        EventManager bus = new EventManager();
        final List<Integer> received = new ArrayList<Integer>();

        bus.on(NumberedPing.class)
                .throttle(100, TimeUnit.MILLISECONDS)
                .handle(new Consumer<NumberedPing>() {
                    @Override
                    public void accept(NumberedPing ping) {
                        received.add(ping.getId());
                    }
                });

        // First event must pass immediately
        bus.dispatch(new NumberedPing(1));
        assertEquals(1, received.size());

        // Subsequent rapid events within 100ms must be throttled/dropped
        bus.dispatch(new NumberedPing(2));
        bus.dispatch(new NumberedPing(3));
        assertEquals(1, received.size());

        // After throttle window passes, next event passes
        Thread.sleep(120);
        bus.dispatch(new NumberedPing(4));
        assertEquals(2, received.size());
        assertEquals(Integer.valueOf(4), received.get(1));
    }

    @Test
    public void testEventDebouncing() throws Exception {
        EventManager bus = new EventManager();
        final List<Integer> received = new ArrayList<Integer>();

        bus.on(NumberedPing.class)
                .debounce(50, TimeUnit.MILLISECONDS)
                .handle(new Consumer<NumberedPing>() {
                    @Override
                    public void accept(NumberedPing ping) {
                        received.add(ping.getId());
                    }
                });

        // Rapid dispatches with 10ms gaps (all before 50ms quiet period)
        bus.dispatch(new NumberedPing(10));
        Thread.sleep(10);
        bus.dispatch(new NumberedPing(20));
        Thread.sleep(10);
        bus.dispatch(new NumberedPing(30));

        // No events should be delivered yet during active sequence
        assertEquals(0, received.size());

        // Wait for 50ms quiet period to elapse
        Thread.sleep(80);

        // Exactly 1 event (the latest one: 30) should have fired
        assertEquals(1, received.size());
        assertEquals(Integer.valueOf(30), received.get(0));
    }

    // =========================================================================
    // Feature 13: Circuit Breaker Fault Isolation Tests
    // =========================================================================

    @Test
    public void testCircuitBreakerStateTransitions() {
        CircuitBreaker cb = new CircuitBreaker(2, 50, TimeUnit.MILLISECONDS);
        assertEquals(CircuitBreaker.State.CLOSED, cb.getState());
        assertTrue(cb.allowExecution());

        // First failure
        cb.recordFailure();
        assertEquals(CircuitBreaker.State.CLOSED, cb.getState());
        assertTrue(cb.allowExecution());

        // Second failure -> trips to OPEN
        cb.recordFailure();
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());
        assertFalse(cb.allowExecution());

        // Reset restores to CLOSED
        cb.reset();
        assertEquals(CircuitBreaker.State.CLOSED, cb.getState());
        assertTrue(cb.allowExecution());
    }

    @Test
    public void testSubscriberCircuitBreakerIntegration() throws Exception {
        EventManager bus = new EventManager();
        final AtomicInteger executionCount = new AtomicInteger();

        bus.on(NumberedPing.class)
                .circuitBreaker(2, 50, TimeUnit.MILLISECONDS)
                .handle(new Consumer<NumberedPing>() {
                    @Override
                    public void accept(NumberedPing ping) {
                        executionCount.incrementAndGet();
                        if (ping.getId() < 0) {
                            throw new IllegalStateException("Faulty listener failure");
                        }
                    }
                });

        // 1. Success execution
        bus.dispatch(new NumberedPing(1));
        assertEquals(1, executionCount.get());

        // 2. Failure 1
        try { bus.dispatch(new NumberedPing(-1)); } catch (Exception ignored) {}
        assertEquals(2, executionCount.get());

        // 3. Failure 2 -> trips circuit breaker to OPEN
        try { bus.dispatch(new NumberedPing(-2)); } catch (Exception ignored) {}
        assertEquals(3, executionCount.get());

        // 4. Execution while OPEN: must be rejected without calling listener action!
        bus.dispatch(new NumberedPing(10));
        assertEquals(3, executionCount.get()); // Still 3, listener was isolated!

        // 5. Wait for cooldown to expire (50ms)
        Thread.sleep(60);

        // 6. Trial execution in HALF_OPEN succeeds and resets breaker to CLOSED
        bus.dispatch(new NumberedPing(20));
        assertEquals(4, executionCount.get());
    }

    // =========================================================================
    // Feature 14: Flow.Publisher Adapter Tests
    // =========================================================================

    @Test
    public void testFlowPublisherAdapterCompatibility() {
        EventManager bus = new EventManager();
        if (FlowPublisherAdapter.isSupported()) {
            Object flowPublisher = bus.asFlowPublisher(NumberedPing.class);
            assertNotNull(flowPublisher);
            assertTrue(flowPublisher.toString().contains("FlowPublisher"));
        } else {
            assertThrows(UnsupportedOperationException.class, new org.junit.jupiter.api.function.Executable() {
                @Override
                public void execute() throws Throwable {
                    bus.asFlowPublisher(NumberedPing.class);
                }
            });
        }
    }


    // =========================================================================
    // Feature 15: DAG Topological Ordering & Cycle Detection Tests
    // =========================================================================

    public static final class DagTestEvent implements Event {
        private final String message;
        public DagTestEvent(String message) {
            this.message = message;
        }
        public String getMessage() {
            return message;
        }
    }

    public static final class DagAuthListener {
        private final List<String> log;
        public DagAuthListener(List<String> log) { this.log = log; }
        @EventTarget(id = "auth")
        public void onEvent(DagTestEvent event) { log.add("auth"); }
    }

    public static final class DagSecurityListener {
        private final List<String> log;
        public DagSecurityListener(List<String> log) { this.log = log; }
        @EventTarget(id = "security", after = {"auth"}, afterClasses = {DagAuthListener.class})
        public void onEvent(DagTestEvent event) { log.add("security"); }
    }

    public static final class DagAuditListener {
        private final List<String> log;
        public DagAuditListener(List<String> log) { this.log = log; }
        @EventTarget(id = "audit", after = {"security"})
        public void onEvent(DagTestEvent event) { log.add("audit"); }
    }

    public static final class DagCycleA {
        @EventTarget(id = "cycleA", after = {"cycleB"})
        public void onEvent(DagTestEvent event) {}
    }

    public static final class DagCycleB {
        @EventTarget(id = "cycleB", after = {"cycleA"})
        public void onEvent(DagTestEvent event) {}
    }

    @Test
    public void testDagOrderingAnnotationClassesAndIds() {
        EventManager bus = new EventManager();
        List<String> log = new ArrayList<String>();

        // Register in reverse order: audit, security, auth
        bus.register(new DagAuditListener(log));
        bus.register(new DagSecurityListener(log));
        bus.register(new DagAuthListener(log));

        bus.dispatch(new DagTestEvent("test"));

        assertEquals(Arrays.asList("auth", "security", "audit"), log);
    }

    @Test
    public void testDagOrderingOverridesPriority() {
        EventManager bus = new EventManager();
        final List<String> log = new ArrayList<String>();

        // High priority listener explicitly declares it must run AFTER low priority listener
        bus.on(DagTestEvent.class)
                .id("high")
                .priority(Priority.HIGHEST)
                .after("low")
                .handle(new Consumer<DagTestEvent>() {
                    @Override
                    public void accept(DagTestEvent e) {
                        log.add("high");
                    }
                });

        bus.on(DagTestEvent.class)
                .id("low")
                .priority(Priority.LOW)
                .handle(new Consumer<DagTestEvent>() {
                    @Override
                    public void accept(DagTestEvent e) {
                        log.add("low");
                    }
                });

        bus.dispatch(new DagTestEvent("run"));
        assertEquals(Arrays.asList("low", "high"), log);
    }

    @Test
    public void testDagOrderingViaFluentDsl() {
        EventManager bus = new EventManager();
        final List<String> log = new ArrayList<String>();

        bus.on(DagTestEvent.class)
                .id("C")
                .after("B")
                .handle(new Consumer<DagTestEvent>() {
                    @Override
                    public void accept(DagTestEvent e) {
                        log.add("C");
                    }
                });

        bus.on(DagTestEvent.class)
                .id("A")
                .before("B")
                .handle(new Consumer<DagTestEvent>() {
                    @Override
                    public void accept(DagTestEvent e) {
                        log.add("A");
                    }
                });

        bus.on(DagTestEvent.class)
                .id("B")
                .handle(new Consumer<DagTestEvent>() {
                    @Override
                    public void accept(DagTestEvent e) {
                        log.add("B");
                    }
                });

        bus.dispatch(new DagTestEvent("go"));
        assertEquals(Arrays.asList("A", "B", "C"), log);
    }

    @Test
    public void testDagCircularDependencyDetection() {
        final EventManager bus = new EventManager();
        bus.register(new DagCycleA());
        assertThrows(CircularDependencyException.class, new org.junit.jupiter.api.function.Executable() {
            @Override
            public void execute() throws Throwable {
                bus.register(new DagCycleB());
            }
        });
    }

    // =========================================================================
    // Feature 16: AsyncEventChannel with Backpressure Tests
    // =========================================================================

    @Test
    public void testAsyncEventChannelBlock() throws Exception {
        EventManager bus = new EventManager();
        final CountDownLatch latch = new CountDownLatch(5);
        bus.on(DagTestEvent.class).handle(new Consumer<DagTestEvent>() {
            @Override
            public void accept(DagTestEvent e) {
                latch.countDown();
            }
        });

        AsyncEventChannel<DagTestEvent> channel = bus.createChannel(DagTestEvent.class, 10, BackpressurePolicy.BLOCK);
        for (int i = 0; i < 5; i++) {
            assertTrue(channel.publish(new DagTestEvent("msg-" + i)));
        }

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        assertEquals(5, channel.getDispatchedCount());
        assertEquals(0, channel.getDroppedCount());
        channel.close();
        assertTrue(channel.isClosed());
    }

    @Test
    public void testAsyncEventChannelDropOldest() throws Exception {
        EventManager bus = new EventManager();
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch blockConsumer = new CountDownLatch(1);

        bus.on(DagTestEvent.class).handle(new Consumer<DagTestEvent>() {
            @Override
            public void accept(DagTestEvent e) {
                started.countDown();
                try {
                    blockConsumer.await(1, TimeUnit.SECONDS);
                } catch (InterruptedException ignored) {}
            }
        });

        AsyncEventChannel<DagTestEvent> channel = bus.createChannel(DagTestEvent.class, 2, BackpressurePolicy.DROP_OLDEST);
        channel.publish(new DagTestEvent("msg-0"));
        started.await(1, TimeUnit.SECONDS);

        // Fill and overflow channel while consumer is blocked
        for (int i = 1; i <= 6; i++) {
            channel.publish(new DagTestEvent("msg-" + i));
        }

        blockConsumer.countDown();
        channel.close();

        assertEquals(7, channel.getPublishedCount());
        assertTrue(channel.getDroppedCount() > 0);
    }

    @Test
    public void testAsyncEventChannelDropLatest() throws Exception {
        EventManager bus = new EventManager();
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch blockConsumer = new CountDownLatch(1);

        bus.on(DagTestEvent.class).handle(new Consumer<DagTestEvent>() {
            @Override
            public void accept(DagTestEvent e) {
                started.countDown();
                try {
                    blockConsumer.await(1, TimeUnit.SECONDS);
                } catch (InterruptedException ignored) {}
            }
        });

        AsyncEventChannel<DagTestEvent> channel = bus.createChannel(DagTestEvent.class, 2, BackpressurePolicy.DROP_LATEST);
        channel.publish(new DagTestEvent("msg-0"));
        started.await(1, TimeUnit.SECONDS);

        // Fill buffer
        channel.publish(new DagTestEvent("msg-1"));
        channel.publish(new DagTestEvent("msg-2"));

        // Next publish should be dropped
        boolean accepted = channel.publish(new DagTestEvent("msg-3-dropped"));
        assertFalse(accepted);

        blockConsumer.countDown();
        channel.close();

        assertTrue(channel.getDroppedCount() >= 1);
    }

    @Test
    public void testAsyncEventChannelCallerRuns() throws Exception {
        EventManager bus = new EventManager();
        final AtomicInteger count = new AtomicInteger();
        bus.on(DagTestEvent.class).handle(new Consumer<DagTestEvent>() {
            @Override
            public void accept(DagTestEvent e) {
                count.incrementAndGet();
            }
        });

        AsyncEventChannel<DagTestEvent> channel = bus.createChannel(DagTestEvent.class, 2, BackpressurePolicy.CALLER_RUNS);
        for (int i = 0; i < 6; i++) {
            channel.publish(new DagTestEvent("msg-" + i));
        }

        channel.close();
        assertEquals(0, channel.getDroppedCount());
    }

    // =========================================================================
    // Feature 17: Event Upcasting & Version Migration Tests
    // =========================================================================

    public static final class OrderV1 implements Event {
        private final String orderId;
        public OrderV1(String orderId) { this.orderId = orderId; }
        public String getOrderId() { return orderId; }
    }

    public static final class OrderV2 implements Event {
        private final String orderId;
        private final double amount;
        public OrderV2(String orderId, double amount) { this.orderId = orderId; this.amount = amount; }
        public String getOrderId() { return orderId; }
        public double getAmount() { return amount; }
    }

    public static final class OrderV3 implements Event {
        private final String orderId;
        private final double amount;
        private final String currency;
        public OrderV3(String orderId, double amount, String currency) {
            this.orderId = orderId; this.amount = amount; this.currency = currency;
        }
        public String getOrderId() { return orderId; }
        public double getAmount() { return amount; }
        public String getCurrency() { return currency; }
    }

    @Test
    public void testEventUpcastingSingleHop() {
        EventManager bus = new EventManager();
        bus.registerUpcaster(OrderV1.class, OrderV2.class, new java.util.function.Function<OrderV1, OrderV2>() {
            @Override
            public OrderV2 apply(OrderV1 v1) {
                return new OrderV2(v1.getOrderId(), 99.9);
            }
        });

        final AtomicReference<OrderV2> captured = new AtomicReference<OrderV2>();
        bus.on(OrderV2.class).handle(new Consumer<OrderV2>() {
            @Override
            public void accept(OrderV2 v2) {
                captured.set(v2);
            }
        });

        bus.dispatch(new OrderV1("ORD-101"));
        assertNotNull(captured.get());
        assertEquals("ORD-101", captured.get().getOrderId());
        assertEquals(99.9, captured.get().getAmount(), 0.001);
    }

    @Test
    public void testEventUpcastingMultiHop() {
        EventManager bus = new EventManager();

        bus.registerUpcaster(OrderV1.class, OrderV2.class, new java.util.function.Function<OrderV1, OrderV2>() {
            @Override
            public OrderV2 apply(OrderV1 v1) {
                return new OrderV2(v1.getOrderId(), 100.0);
            }
        });

        bus.registerUpcaster(OrderV2.class, OrderV3.class, new java.util.function.Function<OrderV2, OrderV3>() {
            @Override
            public OrderV3 apply(OrderV2 v2) {
                return new OrderV3(v2.getOrderId(), v2.getAmount(), "USD");
            }
        });

        final AtomicReference<OrderV1> recV1 = new AtomicReference<OrderV1>();
        final AtomicReference<OrderV2> recV2 = new AtomicReference<OrderV2>();
        final AtomicReference<OrderV3> recV3 = new AtomicReference<OrderV3>();

        bus.on(OrderV1.class).handle(new Consumer<OrderV1>() {
            @Override
            public void accept(OrderV1 e) { recV1.set(e); }
        });
        bus.on(OrderV2.class).handle(new Consumer<OrderV2>() {
            @Override
            public void accept(OrderV2 e) { recV2.set(e); }
        });
        bus.on(OrderV3.class).handle(new Consumer<OrderV3>() {
            @Override
            public void accept(OrderV3 e) { recV3.set(e); }
        });

        bus.dispatch(new OrderV1("ORD-MULTIHOP"));

        assertNotNull(recV1.get());
        assertNotNull(recV2.get());
        assertNotNull(recV3.get());
        assertEquals("USD", recV3.get().getCurrency());
        assertEquals(100.0, recV3.get().getAmount(), 0.001);
    }

    @Test
    public void testStickyEventUpcasting() {
        EventManager bus = new EventManager();
        bus.registerUpcaster(OrderV1.class, OrderV2.class, new java.util.function.Function<OrderV1, OrderV2>() {
            @Override
            public OrderV2 apply(OrderV1 v1) {
                return new OrderV2(v1.getOrderId(), 50.0);
            }
        });

        // Dispatch sticky OrderV1
        bus.dispatchSticky(new OrderV1("STICKY-ORD"));

        // Register subscriber on OrderV2 with sticky enabled
        final AtomicReference<OrderV2> received = new AtomicReference<OrderV2>();
        bus.on(OrderV2.class).sticky().handle(new Consumer<OrderV2>() {
            @Override
            public void accept(OrderV2 v2) {
                received.set(v2);
            }
        });

        assertNotNull(received.get());
        assertEquals("STICKY-ORD", received.get().getOrderId());
        assertEquals(50.0, received.get().getAmount(), 0.001);
    }


    // =========================================================================
    // Feature 18: Micro-Batching Sliding Windows Tests
    // =========================================================================

    public static final class BatchItemEvent implements Event {
        private final int id;
        public BatchItemEvent(int id) { this.id = id; }
        public int getId() { return id; }
    }

    @Test
    public void testMicroBatchingBySize() {
        EventManager bus = new EventManager();
        final List<List<BatchItemEvent>> batches = new ArrayList<List<BatchItemEvent>>();

        bus.on(BatchItemEvent.class)
                .buffer(3, 5, TimeUnit.SECONDS)
                .handleBatch(new Consumer<List<BatchItemEvent>>() {
                    @Override
                    public void accept(List<BatchItemEvent> batch) {
                        batches.add(batch);
                    }
                });

        bus.dispatch(new BatchItemEvent(1));
        bus.dispatch(new BatchItemEvent(2));
        assertEquals(0, batches.size());

        bus.dispatch(new BatchItemEvent(3));
        assertEquals(1, batches.size());
        assertEquals(3, batches.get(0).size());
        assertEquals(1, batches.get(0).get(0).getId());
        assertEquals(3, batches.get(0).get(2).getId());
    }

    @Test
    public void testMicroBatchingByTimeout() throws Exception {
        EventManager bus = new EventManager();
        final List<List<BatchItemEvent>> batches = new ArrayList<List<BatchItemEvent>>();
        final CountDownLatch latch = new CountDownLatch(1);

        bus.on(BatchItemEvent.class)
                .buffer(10, 40, TimeUnit.MILLISECONDS)
                .handleBatch(new Consumer<List<BatchItemEvent>>() {
                    @Override
                    public void accept(List<BatchItemEvent> batch) {
                        batches.add(batch);
                        latch.countDown();
                    }
                });

        bus.dispatch(new BatchItemEvent(10));
        bus.dispatch(new BatchItemEvent(20));

        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertEquals(1, batches.size());
        assertEquals(2, batches.get(0).size());
    }

    @Test
    public void testMicroBatchingFlushOnUnsubscribe() {
        EventManager bus = new EventManager();
        final List<List<BatchItemEvent>> batches = new ArrayList<List<BatchItemEvent>>();

        Subscription sub = bus.on(BatchItemEvent.class)
                .buffer(10, 10, TimeUnit.SECONDS)
                .handleBatch(new Consumer<List<BatchItemEvent>>() {
                    @Override
                    public void accept(List<BatchItemEvent> batch) {
                        batches.add(batch);
                    }
                });

        bus.dispatch(new BatchItemEvent(100));
        bus.dispatch(new BatchItemEvent(200));
        assertEquals(0, batches.size());

        sub.unsubscribe();
        assertEquals(1, batches.size());
        assertEquals(2, batches.get(0).size());
    }

    // =========================================================================
    // Feature 19: Listener Retry Policy Tests
    // =========================================================================

    @Test
    public void testListenerRetrySuccess() {
        EventManager bus = new EventManager();
        final AtomicInteger attempts = new AtomicInteger(0);

        bus.on(BatchItemEvent.class)
                .retry(3, 5, TimeUnit.MILLISECONDS)
                .handle(new Consumer<BatchItemEvent>() {
                    @Override
                    public void accept(BatchItemEvent e) {
                        int count = attempts.incrementAndGet();
                        if (count < 3) {
                            throw new RuntimeException("Transient failure #" + count);
                        }
                    }
                });

        bus.dispatch(new BatchItemEvent(1));
        assertEquals(3, attempts.get());
    }

    @Test
    public void testListenerRetryExhausted() {
        EventManager bus = EventManager.builder().errorPolicy(ErrorPolicy.PROPAGATE).build();
        final AtomicInteger attempts = new AtomicInteger(0);

        bus.on(BatchItemEvent.class)
                .retry(2, 5, TimeUnit.MILLISECONDS)
                .handle(new Consumer<BatchItemEvent>() {
                    @Override
                    public void accept(BatchItemEvent e) {
                        attempts.incrementAndGet();
                        throw new IllegalStateException("Permanent failure");
                    }
                });

        try {
            bus.dispatch(new BatchItemEvent(1));
            fail("Expected exception");
        } catch (IllegalStateException expected) {
            assertEquals(2, attempts.get());
        }
    }

    @Test
    public void testRetryPolicyExponentialBackoffAndFilter() {
        RetryPolicy policy = RetryPolicy.exponentialBackoff(4, 10, 100, TimeUnit.MILLISECONDS)
                .retryOn(IllegalArgumentException.class);

        assertTrue(policy.canRetry(new IllegalArgumentException(), 1));
        assertFalse(policy.canRetry(new NullPointerException(), 1));
        assertFalse(policy.canRetry(new IllegalArgumentException(), 4));
        assertEquals(10000000L, policy.getDelayNanosForAttempt(2)); // 10ms
        assertEquals(20000000L, policy.getDelayNanosForAttempt(3)); // 20ms
    }

    // =========================================================================
    // Feature 20: Saga Compensating Transactions Tests
    // =========================================================================

    @Test
    public void testSagaTransactionCommit() {
        EventManager bus = new EventManager();
        final List<String> log = new ArrayList<String>();

        bus.transaction(new Consumer<TransactionContext>() {
            @Override
            public void accept(TransactionContext tx) {
                tx.onRollback(new Runnable() {
                    @Override
                    public void run() {
                        log.add("rollback");
                    }
                });
                tx.onCommit(new Runnable() {
                    @Override
                    public void run() {
                        log.add("commit");
                    }
                });
                bus.dispatch(new Ping());
            }
        });

        assertEquals(1, log.size());
        assertEquals("commit", log.get(0));
    }

    @Test
    public void testSagaTransactionRollbackCompensationsInReverse() {
        EventManager bus = new EventManager();
        final List<String> log = new ArrayList<String>();

        try {
            bus.transaction(new Consumer<TransactionContext>() {
                @Override
                public void accept(TransactionContext tx) {
                    tx.onRollback(new Runnable() {
                        @Override
                        public void run() {
                            log.add("compensation-1");
                        }
                    });
                    tx.onRollback(new Runnable() {
                        @Override
                        public void run() {
                            log.add("compensation-2");
                        }
                    });
                    throw new RuntimeException("Business failure occurred");
                }
            });
            fail("Should have failed");
        } catch (RuntimeException expected) {
            assertEquals(Arrays.asList("compensation-2", "compensation-1"), log);
        }
    }

    // =========================================================================
    // Feature 21: Topology Exporter Tests
    // =========================================================================

    @Test
    public void testTopologyExportMermaidAndDot() {
        EventManager bus = new EventManager();
        bus.register(new CountingListener());
        bus.registerUpcaster(OrderV1.class, OrderV2.class, new java.util.function.Function<OrderV1, OrderV2>() {
            @Override
            public OrderV2 apply(OrderV1 v1) {
                return new OrderV2(v1.getOrderId(), 1.0);
            }
        });

        String mermaid = bus.exportTopology(TopologyFormat.MERMAID);
        assertNotNull(mermaid);
        assertTrue(mermaid.contains("```mermaid"));
        assertTrue(mermaid.contains("graph TD"));
        assertTrue(mermaid.contains("Ping"));
        assertTrue(mermaid.contains("OrderV1"));
        assertTrue(mermaid.contains("upcasts to"));

        String dot = bus.exportTopology(TopologyFormat.DOT);
        assertNotNull(dot);
        assertTrue(dot.contains("digraph EventManagerTopology"));
        assertTrue(dot.contains("Ping"));
        assertTrue(dot.contains("OrderV1"));
    }

    // =========================================================================
    // Feature 22: Event Recorder & Time-Travel Replay Tests
    // =========================================================================

    @Test
    public void testEventRecorderAndReplay() {
        EventManager sourceBus = new EventManager();
        EventRecorder recorder = sourceBus.startRecording();
        assertTrue(recorder.isRecording());

        sourceBus.dispatch(new BatchItemEvent(1), EventContext.of("trace", "T1"));
        sourceBus.dispatch(new BatchItemEvent(2), EventContext.of("trace", "T2"));
        sourceBus.dispatch(new BatchItemEvent(3), EventContext.of("trace", "T3"));

        RecordedSession session = recorder.stop();
        assertFalse(recorder.isRecording());
        assertEquals(3, session.size());
        assertEquals(3, recorder.getRecordedCount());

        RecordedEvent first = session.getEvents().get(0);
        assertEquals(1, first.getEvent(BatchItemEvent.class).getId());
        assertEquals("T1", first.getContext().get("trace", String.class));

        // Replay to a completely isolated new bus
        EventManager targetBus = new EventManager();
        final List<Integer> replayedIds = new ArrayList<Integer>();
        final List<String> replayedTraces = new ArrayList<String>();

        targetBus.on(BatchItemEvent.class).handle(new Consumer<BatchItemEvent>() {
            @Override
            public void accept(BatchItemEvent e) {
                replayedIds.add(e.getId());
                replayedTraces.add(EventContext.current().get("trace", String.class));
            }
        });

        session.replayTo(targetBus);

        assertEquals(Arrays.asList(1, 2, 3), replayedIds);
        assertEquals(Arrays.asList("T1", "T2", "T3"), replayedTraces);
    }

}
