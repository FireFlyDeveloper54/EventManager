package com.cubk.event;

/**
 * Common priority constants for {@link com.cubk.event.annotations.EventPriority} and functional
 * registration. Lower values run first.
 */
public final class Priority {
    public static final int HIGHEST = 0;
    public static final int HIGH = 5;
    public static final int NORMAL = 10;
    public static final int LOW = 15;
    public static final int LOWEST = 20;

    private Priority() {
    }
}
