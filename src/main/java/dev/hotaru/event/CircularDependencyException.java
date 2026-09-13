package dev.hotaru.event;

import java.util.Collections;
import java.util.List;

/**
 * Thrown when a circular dependency is detected among event listeners during topological sort.
 */
public class CircularDependencyException extends IllegalStateException {
    private static final long serialVersionUID = 1L;
    private final List<?> cyclePath;

    public CircularDependencyException(String message, List<?> cyclePath) {
        super(message);
        this.cyclePath = cyclePath != null ? Collections.unmodifiableList(cyclePath) : Collections.emptyList();
    }

    public List<?> getCyclePath() {
        return cyclePath;
    }
}
