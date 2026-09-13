package dev.hotaru.event;

import dev.hotaru.event.impl.Event;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Pure JDK topology graph generator exporting event hierarchies, listener subscriptions,
 * DAG ordering constraints, and upcaster migration chains in Mermaid or DOT format.
 */
public final class TopologyExporter {

    private static final char QUOTE = '"';

    private TopologyExporter() {}

    public static String export(EventManager bus, TopologyFormat format) {
        if (format == TopologyFormat.DOT) {
            return exportDot(bus);
        } else {
            return exportMermaid(bus);
        }
    }

    private static String exportMermaid(EventManager bus) {
        StringBuilder sb = new StringBuilder();
        sb.append("```mermaid\n");
        sb.append("graph TD\n");
        sb.append("    classDef event fill:#4A90E2,stroke:#1C5BA6,stroke-width:2px,color:#fff;\n");
        sb.append("    classDef listener fill:#50E3C2,stroke:#0B8A6A,stroke-width:2px,color:#000;\n");
        sb.append("    classDef upcaster fill:#F5A623,stroke:#C27405,stroke-width:2px,color:#fff;\n\n");

        Map<Class<? extends Event>, List<String>> topology = bus.getListenerTopologySnapshot();
        Set<String> declaredEvents = new TreeSet<String>();
        Set<String> declaredListeners = new TreeSet<String>();

        // Subscriptions
        for (Map.Entry<Class<? extends Event>, List<String>> entry : topology.entrySet()) {
            String eventName = entry.getKey().getSimpleName();
            declaredEvents.add(eventName);

            for (String listenerName : entry.getValue()) {
                String safeListenerId = "L_" + listenerName.replaceAll("[^a-zA-Z0-9_]", "_");
                declaredListeners.add(safeListenerId + "[" + QUOTE + listenerName + QUOTE + "]");
                sb.append("    ").append(eventName).append(" --> ").append(safeListenerId).append("\n");
            }
        }

        // Upcaster chains
        Map<Class<?>, List<Class<?>>> upcasterMap = bus.getUpcasterTopologySnapshot();
        for (Map.Entry<Class<?>, List<Class<?>>> entry : upcasterMap.entrySet()) {
            String src = entry.getKey().getSimpleName();
            declaredEvents.add(src);
            for (Class<?> target : entry.getValue()) {
                String tgt = target.getSimpleName();
                declaredEvents.add(tgt);
                sb.append("    ").append(src).append(" -.->|").append(QUOTE).append("upcasts to").append(QUOTE).append("| ").append(tgt).append("\n");
            }
        }

        // Apply classes
        for (String ev : declaredEvents) {
            sb.append("    class ").append(ev).append(" event;\n");
        }

        sb.append("```");
        return sb.toString();
    }

    private static String exportDot(EventManager bus) {
        StringBuilder sb = new StringBuilder();
        sb.append("digraph EventManagerTopology {\n");
        sb.append("    rankdir=LR;\n");
        sb.append("    node [shape=box, style=filled, fontname=").append(QUOTE).append("Helvetica").append(QUOTE).append("];\n\n");

        Map<Class<? extends Event>, List<String>> topology = bus.getListenerTopologySnapshot();
        for (Map.Entry<Class<? extends Event>, List<String>> entry : topology.entrySet()) {
            String eventName = entry.getKey().getSimpleName();
            sb.append("    ").append(QUOTE).append(eventName).append(QUOTE).append(" [fillcolor=").append(QUOTE).append("#4A90E2").append(QUOTE).append(", fontcolor=").append(QUOTE).append("#ffffff").append(QUOTE).append("];\n");
            for (String listenerName : entry.getValue()) {
                sb.append("    ").append(QUOTE).append(listenerName).append(QUOTE).append(" [fillcolor=").append(QUOTE).append("#50E3C2").append(QUOTE).append(", fontcolor=").append(QUOTE).append("#000000").append(QUOTE).append("];\n");
                sb.append("    ").append(QUOTE).append(eventName).append(QUOTE).append(" -> ").append(QUOTE).append(listenerName).append(QUOTE).append(";\n");
            }
        }

        Map<Class<?>, List<Class<?>>> upcasterMap = bus.getUpcasterTopologySnapshot();
        for (Map.Entry<Class<?>, List<Class<?>>> entry : upcasterMap.entrySet()) {
            String src = entry.getKey().getSimpleName();
            for (Class<?> target : entry.getValue()) {
                String tgt = target.getSimpleName();
                sb.append("    ").append(QUOTE).append(src).append(QUOTE).append(" -> ").append(QUOTE).append(tgt).append(QUOTE).append(" [style=dashed, label=").append(QUOTE).append("upcasts").append(QUOTE).append("];\n");
            }
        }

        sb.append("}\n");
        return sb.toString();
    }
}
