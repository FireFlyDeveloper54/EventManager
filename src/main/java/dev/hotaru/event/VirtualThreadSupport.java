package dev.hotaru.event;

import java.lang.reflect.Method;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Provides reflection-based native integration with Project Loom (Virtual Threads)
 * introduced in JDK 21+, while maintaining 100% binary and runtime compatibility with
 * JDK 8 through JDK 20.
 */
public final class VirtualThreadSupport {

    private static final Logger log = Logger.getLogger(VirtualThreadSupport.class.getName());
    private static final boolean SUPPORTED;
    private static final Method NEW_VIRTUAL_THREAD_PER_TASK_EXECUTOR;
    private static final Method OF_VIRTUAL;
    private static final Method FACTORY_METHOD;

    static {
        boolean supported = false;
        Method newExecutorMethod = null;
        Method ofVirtualMethod = null;
        Method factoryMethod = null;
        try {
            // Check for java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()
            newExecutorMethod = java.util.concurrent.Executors.class.getMethod("newVirtualThreadPerTaskExecutor");
            // Check for Thread.ofVirtual().factory()
            ofVirtualMethod = Thread.class.getMethod("ofVirtual");
            Class<?> ofVirtualClass = ofVirtualMethod.getReturnType();
            factoryMethod = ofVirtualClass.getMethod("factory");
            supported = true;
        } catch (Throwable ignored) {
            // Virtual threads not available on JDK < 21
        }
        SUPPORTED = supported;
        NEW_VIRTUAL_THREAD_PER_TASK_EXECUTOR = newExecutorMethod;
        OF_VIRTUAL = ofVirtualMethod;
        FACTORY_METHOD = factoryMethod;
    }

    private VirtualThreadSupport() {}

    /**
     * Returns whether Project Loom virtual threads are supported on the current JVM (JDK 21+).
     *
     * @return true if virtual threads are available
     */
    public static boolean isSupported() {
        return SUPPORTED;
    }

    /**
     * Creates an executor that starts a new virtual thread for each task if supported on the current JVM,
     * or gracefully falls back to {@link ForkJoinPool#commonPool()} on earlier JDK versions.
     *
     * @return an executor backed by virtual threads, or the platform common pool as fallback
     */
    public static Executor createVirtualThreadExecutor() {
        if (SUPPORTED && NEW_VIRTUAL_THREAD_PER_TASK_EXECUTOR != null) {
            try {
                return (Executor) NEW_VIRTUAL_THREAD_PER_TASK_EXECUTOR.invoke(null);
            } catch (Throwable t) {
                log.log(Level.WARNING, "Failed to instantiate virtual thread executor, falling back to ForkJoinPool", t);
            }
        }
        return ForkJoinPool.commonPool();
    }

    /**
     * Creates a virtual thread factory if supported on the current JVM, or returns
     * {@link java.util.concurrent.Executors#defaultThreadFactory()} as fallback.
     *
     * @return a virtual thread factory, or default platform factory as fallback
     */
    public static ThreadFactory createVirtualThreadFactory() {
        if (SUPPORTED && OF_VIRTUAL != null && FACTORY_METHOD != null) {
            try {
                Object builder = OF_VIRTUAL.invoke(null);
                return (ThreadFactory) FACTORY_METHOD.invoke(builder);
            } catch (Throwable t) {
                log.log(Level.WARNING, "Failed to instantiate virtual thread factory, falling back to defaultThreadFactory", t);
            }
        }
        return java.util.concurrent.Executors.defaultThreadFactory();
    }
}
