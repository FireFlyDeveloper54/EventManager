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
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    void clearReleasesScanCachesAndStaysUsable() throws Exception {
        EventManager events = new EventManager();
        events.register(new CountingListener());
        events.register(new FieldListener());
        events.call(new Ping());

        assertFalse(cacheOf(events, "listenerPlans").isEmpty());
        assertFalse(cacheOf(events, "invokerFactories").isEmpty());

        events.clear();

        assertEquals(0, events.handlerCount());
        assertTrue(cacheOf(events, "listenerPlans").isEmpty());
        assertTrue(cacheOf(events, "invokerFactories").isEmpty());

        CountingListener rebound = new CountingListener();
        events.register(rebound);
        events.call(new Ping());
        assertEquals(1, rebound.getPings());
    }

    private static Map<?, ?> cacheOf(EventManager events, String name) throws Exception {
        Field field = EventManager.class.getDeclaredField(name);
        field.setAccessible(true);
        return (Map<?, ?>) field.get(events);
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
    void privateHandlersAreInvoked() {
        EventManager events = new EventManager();
        PrivateListener listener = new PrivateListener();
        events.register(listener);
        events.call(new Ping());
        assertEquals(1, listener.getPings());
    }

    static class Ping implements Event {
    }

    static final class AdminPing extends Ping {
    }

    static final class Save extends CancellableEvent {
    }

    static final class Halt extends StoppableEvent {
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
