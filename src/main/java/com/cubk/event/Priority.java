package com.cubk.event;

/**
 * Common priority constants for {@link com.cubk.event.annotations.EventPriority} and functional
 * registration. Lower values run first.
 */
public final class Priority {
    /**
     * Reserved annotation sentinel. Do not use this value for listener registration.
     */
    public static final int UNSPECIFIED = Integer.MIN_VALUE;
    public static final int FIRST = Integer.MIN_VALUE + 1;
    public static final int HIGHEST = 0;
    public static final int HIGH = 5;
    public static final int NORMAL = 10;
    public static final int MEDIUM = NORMAL;
    public static final int LOW = 15;
    public static final int LOWEST = 20;
    public static final int LAST = Integer.MAX_VALUE - 1;
    public static final int MONITOR = Integer.MAX_VALUE;

    private Priority() {
    }
}
