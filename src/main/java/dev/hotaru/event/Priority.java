package dev.hotaru.event;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class Priority {
    public static final int UNSPECIFIED = Integer.MIN_VALUE;
    public static final int FIRST = Integer.MIN_VALUE + 1;
    public static final int HIGHEST = 0;
    public static final int HIGH = 5;
    public static final int NORMAL = 10;
    public static final int LOW = 15;
    public static final int LOWEST = 20;
    public static final int LAST = Integer.MAX_VALUE - 1;
    public static final int MONITOR = Integer.MAX_VALUE;
}
