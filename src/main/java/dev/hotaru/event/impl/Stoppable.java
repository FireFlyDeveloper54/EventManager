package dev.hotaru.event.impl;

public interface Stoppable {

    boolean isStopped();

    void setStopped(boolean state);

    default void stop() {
        setStopped(true);
    }

    default void resume() {
        setStopped(false);
    }
}
