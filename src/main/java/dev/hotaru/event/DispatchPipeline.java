package dev.hotaru.event;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Owns the event dispatch pipeline for an {@link EventManager}.
 *
 * <p>This class contains the dispatch algorithm: handler lookup, interceptor
 * chain, cancellable/stoppable semantics, error policy handling, parent
 * bubbling, dead-event publication, upcaster application, metrics recording,
 * and asynchronous dispatch. {@link EventManager} keeps the public API as thin
 * delegates.</p>
 */
final class DispatchPipeline {
    private static final Logger log = Logger.getLogger(DispatchPipeline.class.getName());

    final EventManager owner;

    DispatchPipeline(EventManager owner) {
        this.owner = owner;
    }

    static final ThreadLocal<FramePool> FRAME_POOLS = new ThreadLocal<FramePool>() {
        @Override
        protected FramePool initialValue() {
            return new FramePool();
        }
    };

    /**
     * Dispatches an event to all matching handlers and returns it.
     *
     * <p>Post-dispatch precedence: when upcasters are registered for the event's
     * type, the converted events are dispatched afterwards, and the original
     * event is then considered handled — it neither bubbles to the owner.parent bus
     * nor produces a {@link DeadEvent}. Otherwise the event bubbles to the
     * owner.parent bus (unless cancelled or stopped), and only if no owner.parent and no
     * handler exists may a {@link DeadEvent} be emitted.
     *
     * @param event the event to dispatch
     * @param <T>   the event type
     * @return the dispatched event, for chaining
     */
    public <T extends Event> T dispatch(T event) {
        if (event == null) {
            return event;
        }
        owner.ensureOpen();

        owner.handlerRegistry.maybePurgeDeadHandlers();

        EventTransactions.EventTransaction tx = EventTransactions.current();
        if (tx != null) {
            tx.buffer.add(new EventTransactions.BufferedEvent(event, false));
            return event;
        }

        owner.checkThreadAffinity(event);

        long startNanos = owner.busMetrics.isEnabled() ? System.nanoTime() : 0L;
        if (owner.busMetrics.isEnabled()) {
            owner.busMetrics.recordDispatch(event.getClass());
        }

        List<EventUpcaster.Typed<?, ?>> upcasterList = owner.upcasterRegistry.forType(event.getClass());
        Handler[] handlers = owner.handlerRegistry.handlersFor(event.getClass());
        if (handlers.length == 0) {
            if (owner.interceptor != null) {
                dispatch(event, Handler.NO_HANDLERS);
            }
            if (owner.busMetrics.isEnabled()) {
                owner.busMetrics.recordDuration(event.getClass(), System.nanoTime() - startNanos);
            }
            owner.upcasterRegistry.applyUpcasters(event, upcasterList, this::dispatch);
            if (canBubbleToParent(event)) {
                owner.parent.dispatch(event);
            } else if (owner.parent == null && owner.deadEventsEnabled && !(event instanceof DeadEvent) && owner.hasListeners(DeadEvent.class)) {
                dispatch(new DeadEvent(owner, event, EventTrace.capture()));
            }
            return event;
        }

        try {
            dispatch(event, handlers);
        } finally {
            if (owner.busMetrics.isEnabled()) {
                owner.busMetrics.recordDuration(event.getClass(), System.nanoTime() - startNanos);
            }
        }

        owner.upcasterRegistry.applyUpcasters(event, upcasterList, this::dispatch);
        if (canBubbleToParent(event)) {
            owner.parent.dispatch(event);
        }

        return event;
    }

    /**
     * Returns whether the event may bubble to the owner.parent bus: a owner.parent must
     * exist and the event must not have been cancelled or stopped.
     */
    boolean canBubbleToParent(Event event) {
        if (owner.parent == null) {
            return false;
        }
        if (event instanceof Cancellable && ((Cancellable) event).isCancelled()) {
            return false;
        }
        if (event instanceof Stoppable && ((Stoppable) event).isStopped()) {
            return false;
        }
        return true;
    }

    public <T extends Event> T dispatch(T event, Runnable afterDispatch) {
        if (event == null) {
            return null;
        }
                owner.ensureOpen();

        owner.handlerRegistry.maybePurgeDeadHandlers();
try {
            return dispatch(event);
        } finally {
            if (afterDispatch != null) {
                afterDispatch.run();
            }
        }
    }

    public <T extends Event> T dispatchExact(T event) {
        if (event == null) {
            return event;
        }
        owner.ensureOpen();

        owner.handlerRegistry.maybePurgeDeadHandlers();

        EventTransactions.EventTransaction tx = EventTransactions.current();
        if (tx != null) {
            tx.buffer.add(new EventTransactions.BufferedEvent(event, true));
            return event;
        }

        owner.checkThreadAffinity(event);

        long startNanos = owner.busMetrics.isEnabled() ? System.nanoTime() : 0L;
        if (owner.busMetrics.isEnabled()) {
            owner.busMetrics.recordDispatch(event.getClass());
        }

        Handler[] handlers = owner.handlerRegistry.exactHandlersFor(event.getClass());
        if (handlers.length != 0) {
            try {
                dispatch(event, handlers);
            } finally {
                if (owner.busMetrics.isEnabled()) {
                    owner.busMetrics.recordDuration(event.getClass(), System.nanoTime() - startNanos);
                }
            }
        } else {
            if (owner.interceptor != null) {
                dispatch(event, Handler.NO_HANDLERS);
            }
            if (owner.busMetrics.isEnabled()) {
                owner.busMetrics.recordDuration(event.getClass(), System.nanoTime() - startNanos);
            }
            if (owner.parent != null) {
                owner.parent.dispatchExact(event);
            } else if (owner.deadEventsEnabled && !(event instanceof DeadEvent) && owner.hasListeners(DeadEvent.class)) {
                dispatch(new DeadEvent(owner, event, EventTrace.capture()));
            }
        }
        return event;
    }

    public <T extends Event> T dispatchExact(T event, Runnable afterDispatch) {
        if (event == null) {
            return null;
        }
                owner.ensureOpen();

        owner.handlerRegistry.maybePurgeDeadHandlers();
try {
            return dispatchExact(event);
        } finally {
            if (afterDispatch != null) {
                afterDispatch.run();
            }
        }
    }

    /**
     * Dispatches a cancellable event and returns whether it was cancelled.
     */
    public <T extends Event & Cancellable> boolean dispatchCancelled(T event) {
        if (event == null) {
            return false;
        }
                owner.ensureOpen();

        owner.handlerRegistry.maybePurgeDeadHandlers();
dispatch(event);
        return event.isCancelled();
    }

    /**
     * Dispatches multiple events in sequential order.
     */
    public void dispatchAll(Event... events) {
        if (events == null || events.length == 0) {
            return;
        }
        owner.ensureOpen();

        owner.handlerRegistry.maybePurgeDeadHandlers();
        for (int i = 0; i < events.length; i++) {
            Event event = events[i];
            if (event != null) {
                dispatch(event);
            }
        }
    }

    /**
     * Dispatches an iterable collection of events in sequential order.
     */
    public void dispatchAll(Iterable<? extends Event> events) {
        if (events == null) {
            return;
        }
        owner.ensureOpen();

        owner.handlerRegistry.maybePurgeDeadHandlers();
        for (Event event : events) {
            if (event != null) {
                dispatch(event);
            }
        }
    }

    /**
     * Dispatches an event asynchronously using ForkJoinPool.commonPool().
     */
    public <T extends Event> CompletableFuture<T> dispatchAsync(final T event) {
        return dispatchAsync(event, owner.defaultExecutor);
    }

    /**
     * Dispatches an event through the supplied executor. The returned future
     * completes with the same event instance after dispatch finishes.
     */
    public <T extends Event> CompletableFuture<T> dispatchAsync(final T event, Executor executor) {
        return submit(event, executor, false);
    }

    /**
     * Dispatches an event to exact-type handlers asynchronously using ForkJoinPool.commonPool().
     */
    public <T extends Event> CompletableFuture<T> dispatchExactAsync(final T event) {
        return dispatchExactAsync(event, owner.defaultExecutor);
    }

    /**
     * Dispatches an event to exact-type handlers through the supplied executor.
     */
    public <T extends Event> CompletableFuture<T> dispatchExactAsync(final T event, Executor executor) {
        return submit(event, executor, true);
    }

    <T extends Event> CompletableFuture<T> submit(final T event, Executor executor,
                                                            final boolean exact) {
        final CompletableFuture<T> future = new CompletableFuture<T>();
        if (event == null) {
            future.complete(null);
            return future;
        }
        if (owner.closed) {
            future.completeExceptionally(new IllegalStateException("EventManager is closed"));
            return future;
        }
        if (executor == null) {
            future.completeExceptionally(new NullPointerException("executor"));
            return future;
        }
        final EventContext callingContext = EventContext.current();
        try {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    EventContext.Scope scope = callingContext != null && callingContext != EventContext.empty()
                            ? callingContext.attach()
                            : null;
                    try {
                        future.complete(exact ? dispatchExact(event) : dispatch(event));
                    } catch (Throwable t) {
                        future.completeExceptionally(t);
                    } finally {
                        if (scope != null) {
                            scope.close();
                        }
                    }
                }
            });
        } catch (Throwable submissionFailure) {
            future.completeExceptionally(submissionFailure);
        }
        return future;
    }

    public <T extends Event> T dispatch(Class<T> eventType, Supplier<T> supplier) {
        if (eventType == null || supplier == null || !owner.hasListeners(eventType)) {
            return null;
        }
        owner.ensureOpen();

        owner.handlerRegistry.maybePurgeDeadHandlers();
        return dispatch(supplier.get());
    }

    public <T extends Event> T dispatchExact(Class<T> eventType, Supplier<T> supplier) {
        if (eventType == null || supplier == null || !owner.hasExactListeners(eventType)) {
            return null;
        }
        owner.ensureOpen();

        owner.handlerRegistry.maybePurgeDeadHandlers();
        return dispatchExact(supplier.get());
    }

    void dispatch(final Event event, final Handler[] handlers) {
        EventInterceptor currentInterceptor = owner.interceptor;
        if (currentInterceptor != null) {
            FramePool pool = FRAME_POOLS.get();
            DispatchFrame frame = pool.acquire(this, event, handlers);
            try {
                currentInterceptor.intercept(event, frame);
            } finally {
                if (frame.executed) {
                    pool.release(frame);
                }
            }
        } else {
            doDispatch(event, handlers);
        }
    }

    void doDispatch(Event event, Handler[] handlers) {
        Cancellable cancellable = event instanceof Cancellable ? (Cancellable) event : null;
        Stoppable stoppable = event instanceof Stoppable ? (Stoppable) event : null;
        boolean stoppableReadable = true;
        boolean cancellableReadable = true;
        List<Throwable> aggregatedErrors = (owner.errorPolicy == ErrorPolicy.AGGREGATE) ? new ArrayList<Throwable>() : null;

        for (int i = 0; i < handlers.length; i++) {
            Handler handler = handlers[i];

            // A dispatch snapshot may outlive a concurrent unregister/clear.
            if (!handler.active.get()) {
                continue;
            }

            if (stoppable != null && stoppableReadable) {
                boolean stopped;
                try {
                    stopped = stoppable.isStopped();
                } catch (Throwable t) {
                    if (!handleFailure(event, event, t, aggregatedErrors)) return;
                    // Under CONTINUE or AGGREGATE, treat an unreadable state as false for
                    // this handler instead of repeatedly invoking the broken
                    // accessor for every remaining handler.
                    stopped = false;
                    stoppableReadable = false;
                }
                if (stopped) {
                    break;
                }
            }

            boolean handling;
            try {
                handling = handler.isHandlingEvents(event) && handler.matchesGenericEvent(event);
            } catch (Throwable t) {
                if (!handleFailure(event, handler.listener, t, aggregatedErrors)) return;
                continue;
            }
            if (!handling) {
                continue;
            }

            if (cancellable != null && handler.ignoreCancelled && cancellableReadable) {
                boolean cancelled;
                try {
                    cancelled = cancellable.isCancelled();
                } catch (Throwable t) {
                    if (!handleFailure(event, event, t, aggregatedErrors)) return;
                    // Under CONTINUE or AGGREGATE, treat an unreadable state as false for
                    // this handler; the failure has already been reported.
                    cancelled = false;
                    cancellableReadable = false;
                }
                if (cancelled) {
                    continue;
                }
            }

            if (handler.filter != null) {
                boolean accepted;
                try {
                    accepted = handler.filter.test(event);
                } catch (Throwable t) {
                    if (!handleFailure(event, handler.listener, t, aggregatedErrors)) return;
                    continue;
                }
                if (!accepted) {
                    continue;
                }
            }

            try {
                if (handler.once) {
                    // Claim the single execution right atomically: concurrent
                    // dispatches sharing this snapshot must not double-fire.
                    if (!handler.active.compareAndSet(true, false)) {
                        continue;
                    }
                    owner.handlerRegistry.removeHandlers(handler.eventType, new Predicate<Handler>() {
                        @Override
                        public boolean test(Handler candidate) {
                            return candidate == handler;
                        }
                    });
                }
                if (owner.busMetrics.isEnabled()) {
                    owner.busMetrics.recordInvocation(event.getClass());
                }
                handler.invoke(event);
            } catch (Throwable t) {
                if (!handleFailure(event, handler.listener, t, aggregatedErrors)) return;
            }
        }

        if (aggregatedErrors != null && !aggregatedErrors.isEmpty()) {
            throw buildAggregatedException(event, aggregatedErrors);
        }
    }

    boolean handleFailure(Event event, Object source, Throwable throwable) {
        return handleFailure(event, source, throwable, null);
    }

    boolean handleFailure(Event event, Object source, Throwable throwable, List<Throwable> aggregatedErrors) {
        if (owner.busMetrics.isEnabled()) {
            owner.busMetrics.recordFailure(event == null ? null : event.getClass());
        }
        try {
            owner.errorHandler.handle(event, source, throwable);
        } catch (Throwable errorHandlerFailure) {
            log.log(Level.SEVERE, "Event error handler threw while handling a listener failure",
                    errorHandlerFailure);
        }
        ErrorPolicy policy = owner.errorPolicy;
        if (policy == ErrorPolicy.PROPAGATE) {
            throw propagate(event, source, throwable);
        }
        if (policy == ErrorPolicy.AGGREGATE && aggregatedErrors != null) {
            aggregatedErrors.add(throwable);
            return true;
        }
        return policy == ErrorPolicy.CONTINUE;
    }

    EventDispatchException buildAggregatedException(Event event, List<Throwable> errors) {
        Throwable primary = errors.get(0);
        String eventName = event == null ? "null" : event.getClass().getName();
        EventDispatchException aggregated = new EventDispatchException(
                "Dispatch of " + eventName + " failed with " + errors.size() + " exception(s)",
                event,
                primary
        );
        for (int i = 1; i < errors.size(); i++) {
            aggregated.addSuppressed(errors.get(i));
        }
        return aggregated;
    }

    static RuntimeException propagate(Event event, Object source, Throwable throwable) {
        if (throwable instanceof RuntimeException) {
            return (RuntimeException) throwable;
        }
        if (throwable instanceof Error) {
            throw (Error) throwable;
        }
        return new EventDispatchException(event, source, throwable);
    }

    /**
     * Dispatches an event within the scope of the specified {@link EventContext}.
     * The context is bound to the thread during dispatch and can be retrieved
     * by listeners via {@link EventContext#current()}.
     *
     * @param event   the event to dispatch
     * @param context the metadata context
     * @param <T>     the event type
     * @return the dispatched event
     */
    public <T extends Event> T dispatch(T event, EventContext context) {
        if (context == null || context == EventContext.empty()) {
            return dispatch(event);
        }
        owner.ensureOpen();

        owner.handlerRegistry.maybePurgeDeadHandlers();
        EventContext.Scope scope = context.attach();
        try {
            return dispatch(event);
        } finally {
            scope.close();
        }
    }

    /**
     * Dispatches an event to exact handlers within the scope of the specified {@link EventContext}.
     */
    public <T extends Event> T dispatchExact(T event, EventContext context) {
        if (context == null || context == EventContext.empty()) {
            return dispatchExact(event);
        }
        owner.ensureOpen();

        owner.handlerRegistry.maybePurgeDeadHandlers();
        EventContext.Scope scope = context.attach();
        try {
            return dispatchExact(event);
        } finally {
            scope.close();
        }
    }

    /**
     * Dispatches an event and caches it as the latest sticky event for its concrete runtime type.
     * Newly registered listeners marked as sticky will immediately receive this cached event upon registration.
     *
     * @param event the event to dispatch and cache
     * @param <T>   the event type
     * @return the dispatched event
     */
    public <T extends Event> T dispatchSticky(T event) {
        if (event == null) {
            return null;
        }
                owner.ensureOpen();

        owner.handlerRegistry.maybePurgeDeadHandlers();
EventTransactions.EventTransaction tx = EventTransactions.current();
        if (tx != null) {
            // Buffer the sticky write together with the dispatch: on rollback
            // the sticky cache must not retain an event that never happened.
            tx.buffer.add(new EventTransactions.BufferedEvent(event, false, true));
            return event;
        }
        owner.writeSticky(event);
        return dispatch(event);
    }

    /**
     * Reports a {@link Throwable} that escaped {@link #dispatch} on a background
     * worker thread (for example {@link AsyncEventChannel}'s dispatcher) to the
     * configured {@link EventErrorHandler}. The error policy is deliberately
     * <em>not</em> applied here: propagating would kill the worker thread, so
     * the worker always continues with the next event. This method never throws.
     *
     * <p>Note: listener failures are already reported to the error handler by
     * dispatch itself, so under {@link ErrorPolicy#PROPAGATE} or
     * {@link ErrorPolicy#AGGREGATE} the same failure may be reported twice —
     * once per listener, once at this worker boundary with a {@code null}
     * listener. Failures that dispatch never saw (such as a throwing
     * owner.interceptor) are reported exactly once here.
     */
    void reportAsyncFailure(Event event, Throwable throwable) {
        try {
            owner.errorHandler.handle(event, null, throwable);
        } catch (Throwable errorHandlerFailure) {
            log.log(Level.SEVERE, "Event error handler threw while handling an async dispatch failure",
                    errorHandlerFailure);
        }
    }

    static final class DispatchFrame implements Runnable {
        private DispatchPipeline pipeline;
        private Event event;
        private Handler[] handlers;
        private DispatchFrame next;
        private volatile boolean executed;

        private void init(DispatchPipeline pipeline, Event event, Handler[] handlers) {
            this.pipeline = pipeline;
            this.event = event;
            this.handlers = handlers;
            this.executed = false;
        }

        private void clear() {
            this.pipeline = null;
            this.event = null;
            this.handlers = null;
        }

        @Override
        public void run() {
            synchronized (this) {
                if (executed) {
                    throw new IllegalStateException("DispatchFrame already executed");
                }
                executed = true;
            }
            DispatchPipeline targetPipeline = this.pipeline;
            if (targetPipeline != null) {
                targetPipeline.doDispatch(event, handlers);
            }
        }
    }

    static final class FramePool {
        DispatchFrame head = new DispatchFrame();

        DispatchFrame acquire(DispatchPipeline pipeline, Event event, Handler[] handlers) {
            DispatchFrame frame = head;
            if (frame == null) {
                frame = new DispatchFrame();
            } else {
                head = frame.next;
                frame.next = null;
            }
            frame.init(pipeline, event, handlers);
            return frame;
        }

        void release(DispatchFrame frame) {
            frame.clear();
            frame.next = head;
            head = frame;
        }
    }
}
