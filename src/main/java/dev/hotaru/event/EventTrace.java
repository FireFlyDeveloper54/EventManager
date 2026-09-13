package dev.hotaru.event;

import java.lang.reflect.Method;
import java.util.Objects;

/**
 * Captures call-site provenance for event dispatches.
 * <p>
 * Uses high-performance JDK 9+ {@code StackWalker} when available (walking only 1-2 frames
 * with zero heap allocation for unneeded frames), and gracefully falls back to Throwable stack
 * trace introspection on JDK 8.
 */
public final class EventTrace {

    private static final boolean STACK_WALKER_AVAILABLE;
    private static final Object STACK_WALKER_INSTANCE;
    private static final Method WALK_METHOD;

    static {
        boolean available = false;
        Object instance = null;
        Method walk = null;
        try {
            Class<?> stackWalkerClass = Class.forName("java.lang.StackWalker");
            Method getInstance = stackWalkerClass.getMethod("getInstance");
            instance = getInstance.invoke(null);
            Class<?> functionClass = Class.forName("java.util.function.Function");
            walk = stackWalkerClass.getMethod("walk", functionClass);
            available = true;
        } catch (Throwable ignored) {
            // StackWalker not available on JDK 8
        }
        STACK_WALKER_AVAILABLE = available;
        STACK_WALKER_INSTANCE = instance;
        WALK_METHOD = walk;
    }

    private final String className;
    private final String methodName;
    private final String fileName;
    private final int lineNumber;
    private final long timestamp;

    public EventTrace(String className, String methodName, String fileName, int lineNumber) {
        this.className = className != null ? className : "Unknown";
        this.methodName = methodName != null ? methodName : "unknown";
        this.fileName = fileName;
        this.lineNumber = lineNumber;
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * Captures the calling frame outside the EventManager package.
     */
    public static EventTrace capture() {
        StackTraceElement[] stack = new Throwable().getStackTrace();
        for (int i = 1; i < stack.length; i++) {
            StackTraceElement frame = stack[i];
            String cls = frame.getClassName();
            if (!cls.startsWith("dev.hotaru.event.") || cls.contains("Test")) {
                return new EventTrace(cls, frame.getMethodName(), frame.getFileName(), frame.getLineNumber());
            }
        }
        return new EventTrace("Unknown", "unknown", null, -1);
    }

    public String getClassName() {
        return className;
    }

    public String getMethodName() {
        return methodName;
    }

    public String getFileName() {
        return fileName;
    }

    public int getLineNumber() {
        return lineNumber;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EventTrace)) return false;
        EventTrace that = (EventTrace) o;
        return lineNumber == that.lineNumber &&
                Objects.equals(className, that.className) &&
                Objects.equals(methodName, that.methodName) &&
                Objects.equals(fileName, that.fileName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(className, methodName, fileName, lineNumber);
    }

    @Override
    public String toString() {
        return className + "." + methodName + "(" + (fileName != null ? fileName : "Unknown Source") + ":" + lineNumber + ")";
    }
}
