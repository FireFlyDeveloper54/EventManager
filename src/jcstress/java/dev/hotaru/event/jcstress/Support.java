package dev.hotaru.event.jcstress;

import dev.hotaru.event.annotations.EventTarget;
import dev.hotaru.event.Event;

public final class Support {
    public static final class Ping implements Event {}

    public static final class AnnotatedListener {
        private final java.util.function.Consumer<Ping> probe;

        public AnnotatedListener(java.util.function.Consumer<Ping> probe) {
            this.probe = probe;
        }

        @EventTarget
        public void onPing(Ping p) {
            probe.accept(p);
        }
    }

    private Support() {}
}
