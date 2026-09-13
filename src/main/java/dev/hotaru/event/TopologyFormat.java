package dev.hotaru.event;

/**
 * Supported formats for exporting event bus listener and dependency topology.
 */
public enum TopologyFormat {
    /**
     * GitHub-flavored Markdown Mermaid graph.
     */
    MERMAID,

    /**
     * Graphviz DOT graph format.
     */
    DOT
}
