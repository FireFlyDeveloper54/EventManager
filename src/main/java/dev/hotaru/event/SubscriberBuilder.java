package dev.hotaru.event;

import dev.hotaru.event.impl.Event;

import java.lang.reflect.Type;
import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * A fluent builder for configuring and registering event subscribers.
 * <p>
 * This builder allows arbitrary combinations of priority, filters, execution constraints
 * (e.g. {@link #once()}), weak references ({@link #weak()}), cancellation handling,
 * sticky event replaying, throttling, debouncing, sampling, and circuit breaking
 * without combinatorial overload explosion.
 *
 * @param <T> the event type
 */
public final class SubscriberBuilder<T extends Event> {

    private final EventManager bus;
    private final Class<T> eventType;
    private int priority = Priority.NORMAL;
    private boolean ignoreCancelled = false;
    private boolean once = false;
    private boolean weak = false;
    private boolean sticky = false;
    private Predicate<? super T> filter = null;
    private Type genericType = null;
    private long throttleNanos = 0L;
    private long debounceDelay = 0L;
    private TimeUnit debounceUnit = null;
    private int sampleCount = 0;
    private CircuitBreaker circuitBreaker = null;
    private String id = null;
    private final java.util.List<String> afterIds = new java.util.ArrayList<String>();
    private final java.util.List<String> beforeIds = new java.util.ArrayList<String>();
    private final java.util.List<Class<?>> afterClasses = new java.util.ArrayList<Class<?>>();
    private final java.util.List<Class<?>> beforeClasses = new java.util.ArrayList<Class<?>>();
    private int bufferSize = 0;
    private long bufferTimeout = 0L;
    private TimeUnit bufferUnit = null;
    private RetryPolicy retryPolicy = null;

    SubscriberBuilder(EventManager bus, Class<T> eventType) {
        this.bus = Objects.requireNonNull(bus, "bus");
        this.eventType = Objects.requireNonNull(eventType, "eventType");
    }

    /**
     * Sets the execution priority of the subscriber.
     */
    public SubscriberBuilder<T> priority(int priority) {
        this.priority = priority;
        return this;
    }

    /**
     * Sets whether this subscriber should ignore cancelled events.
     */
    public SubscriberBuilder<T> ignoreCancelled(boolean ignoreCancelled) {
        this.ignoreCancelled = ignoreCancelled;
        return this;
    }

    /**
     * Configures this subscriber to ignore cancelled events.
     */
    public SubscriberBuilder<T> ignoreCancelled() {
        return ignoreCancelled(true);
    }

    /**
     * Sets whether this subscriber should automatically unregister after handling one event.
     */
    public SubscriberBuilder<T> once(boolean once) {
        this.once = once;
        return this;
    }

    /**
     * Configures this subscriber to automatically unregister after handling one event.
     */
    public SubscriberBuilder<T> once() {
        return once(true);
    }

    /**
     * Sets whether this subscriber should be referenced weakly.
     */
    public SubscriberBuilder<T> weak(boolean weak) {
        this.weak = weak;
        return this;
    }

    /**
     * Configures this subscriber to be referenced weakly.
     */
    public SubscriberBuilder<T> weak() {
        return weak(true);
    }

    /**
     * Enables replaying of previously dispatched sticky events of this type upon registration.
     */
    public SubscriberBuilder<T> sticky() {
        return sticky(true);
    }

    /**
     * Configures whether to replay cached sticky events upon registration.
     */
    public SubscriberBuilder<T> sticky(boolean sticky) {
        this.sticky = sticky;
        return this;
    }

    /**
     * Throttles event consumption so that the action is executed at most once per specified interval.
     */
    public SubscriberBuilder<T> throttle(long interval, TimeUnit unit) {
        Objects.requireNonNull(unit, "unit");
        this.throttleNanos = unit.toNanos(interval);
        return this;
    }

    /**
     * Debounces event consumption so that the action is only executed after a quiet period of {@code delay}.
     */
    public SubscriberBuilder<T> debounce(long delay, TimeUnit unit) {
        Objects.requireNonNull(unit, "unit");
        this.debounceDelay = delay;
        this.debounceUnit = unit;
        return this;
    }

    /**
     * Samples event consumption so that the action is executed once every {@code count} events.
     */
    public SubscriberBuilder<T> sample(int count) {
        if (count <= 0) {
            throw new IllegalArgumentException("sample count must be positive");
        }
        this.sampleCount = count;
        return this;
    }

    /**
     * Attaches an automatic fault circuit breaker to protect the bus from repeatedly failing listeners.
     */
    public SubscriberBuilder<T> circuitBreaker(int maxFailures, long cooldown, TimeUnit unit) {
        return circuitBreaker(new CircuitBreaker(maxFailures, cooldown, unit));
    }

    /**
     * Attaches a pre-configured {@link CircuitBreaker}.
     */
    public SubscriberBuilder<T> circuitBreaker(CircuitBreaker circuitBreaker) {
        this.circuitBreaker = circuitBreaker;
        return this;
    }

    /**
     * Adds an event filter predicate. If a filter already exists, the new filter is AND-composed.
     */
    public SubscriberBuilder<T> filter(final Predicate<? super T> filter) {
        if (filter != null) {
            if (this.filter == null) {
                this.filter = filter;
            } else {
                final Predicate<? super T> prev = this.filter;
                this.filter = new Predicate<T>() {
                    @Override
                    public boolean test(T t) {
                        return prev.test(t) && filter.test(t);
                    }
                };
            }
        }
        return this;
    }

    /**
     * Adds an {@link EventFilter}. If a filter already exists, the new filter is AND-composed.
     */
    public SubscriberBuilder<T> filter(final EventFilter<? super T> filter) {
        if (filter != null) {
            return filter(new Predicate<T>() {
                @Override
                public boolean test(T t) {
                    return filter.test(t);
                }
            });
        }
        return this;
    }

    /**
     * Specifies the expected generic type argument when listening to a {@link GenericEvent}.
     */
    public SubscriberBuilder<T> genericType(Type genericType) {
        this.genericType = genericType;
        return this;
    }

    /**
     * Specifies the expected generic type argument via a {@link TypeToken}.
     */
    public SubscriberBuilder<T> genericType(TypeToken<?> typeToken) {
        this.genericType = typeToken != null ? typeToken.getType() : null;
        return this;
    }

    /**
     * Terminal operation: registers a {@link Consumer} action and returns the active subscription.
     */
        /**
     * Assigns a unique identifier to this subscriber for DAG dependency references.
     */
    public SubscriberBuilder<T> id(String id) {
        this.id = id;
        return this;
    }

    /**
     * Declares that this subscriber must execute after the handlers with the given IDs.
     */
    public SubscriberBuilder<T> after(String... ids) {
        if (ids != null) {
            for (String s : ids) {
                if (s != null && !s.isEmpty()) {
                    afterIds.add(s);
                }
            }
        }
        return this;
    }

    /**
     * Declares that this subscriber must execute before the handlers with the given IDs.
     */
    public SubscriberBuilder<T> before(String... ids) {
        if (ids != null) {
            for (String s : ids) {
                if (s != null && !s.isEmpty()) {
                    beforeIds.add(s);
                }
            }
        }
        return this;
    }

    /**
     * Declares that this subscriber must execute after handlers belonging to the specified classes.
     */
    public SubscriberBuilder<T> after(Class<?>... classes) {
        if (classes != null) {
            for (Class<?> c : classes) {
                if (c != null) {
                    afterClasses.add(c);
                }
            }
        }
        return this;
    }

    /**
     * Declares that this subscriber must execute before handlers belonging to the specified classes.
     */
    public SubscriberBuilder<T> before(Class<?>... classes) {
        if (classes != null) {
            for (Class<?> c : classes) {
                if (c != null) {
                    beforeClasses.add(c);
                }
            }
        }
        return this;
    }

        /**
     * Configures sliding-window micro-batching. Events will be collected and delivered as a batch
     * once {@code maxBatchSize} is reached or {@code timeout} elapses.
     *
     * @param maxBatchSize maximum events in a single batch
     * @param timeout quiet or maximum duration before flushing an incomplete batch
     * @param unit time unit
     */
    public SubscriberBuilder<T> buffer(int maxBatchSize, long timeout, TimeUnit unit) {
        if (maxBatchSize <= 0) {
            throw new IllegalArgumentException("maxBatchSize must be positive");
        }
        Objects.requireNonNull(unit, "unit");
        this.bufferSize = maxBatchSize;
        this.bufferTimeout = timeout;
        this.bufferUnit = unit;
        return this;
    }

    /**
     * Configures a fixed-interval retry policy on listener execution failures.
     */
    public SubscriberBuilder<T> retry(int maxAttempts, long delay, TimeUnit unit) {
        return retry(RetryPolicy.fixed(maxAttempts, delay, unit));
    }

    /**
     * Attaches a custom {@link RetryPolicy} (e.g. exponential backoff).
     */
    public SubscriberBuilder<T> retry(RetryPolicy policy) {
        this.retryPolicy = policy;
        return this;
    }

    /**
     * Registers a micro-batching handler consuming batches of events.
     * If {@link #buffer(int, long, TimeUnit)} was not configured, defaults to 100 events or 50ms.
     */
    public Subscription handleBatch(final Consumer<java.util.List<T>> batchAction) {
        Objects.requireNonNull(batchAction, "batchAction");
        final int batchLimit = this.bufferSize > 0 ? this.bufferSize : 100;
        final long timeout = this.bufferTimeout > 0 ? this.bufferTimeout : 50;
        final TimeUnit unit = this.bufferUnit != null ? this.bufferUnit : TimeUnit.MILLISECONDS;

        final java.util.List<T> buffer = new java.util.ArrayList<T>(batchLimit);
        final Object lock = new Object();
        final AtomicReference<ScheduledFuture<?>> flushTask = new AtomicReference<ScheduledFuture<?>>();

        final Runnable flushAction = new Runnable() {
            @Override
            public void run() {
                java.util.List<T> toDispatch = null;
                synchronized (lock) {
                    if (!buffer.isEmpty()) {
                        toDispatch = new java.util.ArrayList<T>(buffer);
                        buffer.clear();
                    }
                    ScheduledFuture<?> task = flushTask.get();
                    if (task != null) {
                        task.cancel(false);
                        flushTask.set(null);
                    }
                }
                if (toDispatch != null && !toDispatch.isEmpty()) {
                    try {
                        batchAction.accept(toDispatch);
                    } catch (Throwable ignored) {}
                }
            }
        };

        Consumer<T> bufferingConsumer = new Consumer<T>() {
            @Override
            public void accept(T event) {
                java.util.List<T> toDispatch = null;
                synchronized (lock) {
                    buffer.add(event);
                    if (buffer.size() >= batchLimit) {
                        toDispatch = new java.util.ArrayList<T>(buffer);
                        buffer.clear();
                        ScheduledFuture<?> task = flushTask.get();
                        if (task != null) {
                            task.cancel(false);
                            flushTask.set(null);
                        }
                    } else if (flushTask.get() == null) {
                        ScheduledFuture<?> next = EventManager.getTimeoutScheduler().schedule(flushAction, timeout, unit);
                        flushTask.set(next);
                    }
                }
                if (toDispatch != null) {
                    try {
                        batchAction.accept(toDispatch);
                    } catch (Throwable ignored) {}
                }
            }
        };

        final Subscription sub = handle(bufferingConsumer);
        return new Subscription() {
            @Override
            public void unsubscribe() {
                sub.unsubscribe();
                flushAction.run(); // Flush remaining items on unsubscribe
            }

            @Override
            public boolean isSubscribed() {
                return sub.isSubscribed();
            }
        };
    }

    public Subscription handle(Consumer<? super T> action) {
        Objects.requireNonNull(action, "action");

        // Wrap with RetryPolicy if configured
        if (this.retryPolicy != null) {
            final RetryPolicy rp = this.retryPolicy;
            final Consumer<? super T> prevAction = action;
            action = new Consumer<T>() {
                @Override
                public void accept(T event) {
                    int attempt = 1;
                    while (true) {
                        try {
                            prevAction.accept(event);
                            return;
                        } catch (Throwable t) {
                            if (rp.canRetry(t, attempt)) {
                                long delayNanos = rp.getDelayNanosForAttempt(attempt + 1);
                                attempt++;
                                if (delayNanos > 0) {
                                    try {
                                        TimeUnit.NANOSECONDS.sleep(delayNanos);
                                    } catch (InterruptedException ie) {
                                        Thread.currentThread().interrupt();
                                        if (t instanceof RuntimeException) throw (RuntimeException) t;
                                        if (t instanceof Error) throw (Error) t;
                                        throw new RuntimeException(t);
                                    }
                                }
                            } else {
                                if (t instanceof RuntimeException) throw (RuntimeException) t;
                                if (t instanceof Error) throw (Error) t;
                                throw new RuntimeException(t);
                            }
                        }
                    }
                }
            };
        }

        // Wrap with CircuitBreaker if configured
        if (this.circuitBreaker != null) {
            final CircuitBreaker cb = this.circuitBreaker;
            final Consumer<? super T> prev = action;
            action = new Consumer<T>() {
                @Override
                public void accept(T event) {
                    if (cb.allowExecution()) {
                        try {
                            prev.accept(event);
                            cb.recordSuccess();
                        } catch (Throwable t) {
                            cb.recordFailure();
                            if (t instanceof RuntimeException) throw (RuntimeException) t;
                            if (t instanceof Error) throw (Error) t;
                            throw new RuntimeException(t);
                        }
                    }
                }
            };
        }

        // Wrap with Sampling if configured
        if (this.sampleCount > 1) {
            final AtomicLong sampleCounter = new AtomicLong();
            final int sc = this.sampleCount;
            final Consumer<? super T> prev = action;
            action = new Consumer<T>() {
                @Override
                public void accept(T event) {
                    if (sampleCounter.incrementAndGet() % sc == 1) {
                        prev.accept(event);
                    }
                }
            };
        }

        // Wrap with Throttle if configured
        if (this.throttleNanos > 0L) {
            final AtomicLong lastRun = new AtomicLong(0L);
            final long tn = this.throttleNanos;
            final Consumer<? super T> prev = action;
            action = new Consumer<T>() {
                @Override
                public void accept(T event) {
                    long now = System.nanoTime();
                    long prevTime = lastRun.get();
                    if (now - prevTime >= tn && lastRun.compareAndSet(prevTime, now)) {
                        prev.accept(event);
                    }
                }
            };
        }

        // Wrap with Debounce if configured
        if (this.debounceDelay > 0L && this.debounceUnit != null) {
            final AtomicReference<ScheduledFuture<?>> debounceTask = new AtomicReference<ScheduledFuture<?>>();
            final Consumer<? super T> prev = action;
            final long delay = this.debounceDelay;
            final TimeUnit unit = this.debounceUnit;
            action = new Consumer<T>() {
                @Override
                public void accept(final T event) {
                    ScheduledFuture<?> prevTask = debounceTask.get();
                    if (prevTask != null) {
                        prevTask.cancel(false);
                    }
                    ScheduledFuture<?> nextTask = EventManager.getTimeoutScheduler().schedule(new Runnable() {
                        @Override
                        public void run() {
                            prev.accept(event);
                        }
                    }, delay, unit);
                    debounceTask.set(nextTask);
                }
            };
        }

        return bus.registerSubscriber(
                eventType, priority, ignoreCancelled, once, weak, sticky, filter, genericType,
                id,
                afterIds.toArray(new String[afterIds.size()]),
                beforeIds.toArray(new String[beforeIds.size()]),
                afterClasses.toArray(new Class<?>[afterClasses.size()]),
                beforeClasses.toArray(new Class<?>[beforeClasses.size()]),
                action
        );
    }

    /**
     * Terminal operation: registers an {@link EventListener} and returns the active subscription.
     */
    public Subscription listener(final EventListener<? super T> listener) {
        Objects.requireNonNull(listener, "listener");
        if (this.priority == Priority.NORMAL && listener.getPriority() != Priority.NORMAL) {
            this.priority = listener.getPriority();
        }
        return handle(listener);
    }
}
