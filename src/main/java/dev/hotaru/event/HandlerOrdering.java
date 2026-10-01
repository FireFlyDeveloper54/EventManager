package dev.hotaru.event;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Orders handlers for dispatch: plain priority/registration order when no DAG
 * constraints exist, otherwise Kahn's topological sort over {@code after}/
 * {@code before} edges with priority as the tie-breaker, and cycle detection
 * that reports the offending path via {@link CircularDependencyException}.
 */
final class HandlerOrdering {

    static final Comparator<Handler> HANDLER_ORDER = new Comparator<Handler>() {
        @Override
        public int compare(Handler left, Handler right) {
            int priorityCompare = Integer.compare(left.priority, right.priority);
            if (priorityCompare != 0) {
                return priorityCompare;
            }
            return Long.compare(left.order, right.order);
        }
    };

    private HandlerOrdering() {
    }

    static Handler[] sortWithDag(List<Handler> handlers) {
        if (handlers == null || handlers.isEmpty()) {
            return Handler.NO_HANDLERS;
        }
        int n = handlers.size();
        if (n == 1) {
            return handlers.toArray(Handler.NO_HANDLERS);
        }

        boolean hasDag = false;
        for (int i = 0; i < n; i++) {
            Handler h = handlers.get(i);
            if ((h.id != null && !h.id.isEmpty())
                    || (h.after != null && h.after.length > 0)
                    || (h.before != null && h.before.length > 0)
                    || (h.afterClasses != null && h.afterClasses.length > 0)
                    || (h.beforeClasses != null && h.beforeClasses.length > 0)) {
                hasDag = true;
                break;
            }
        }

        if (!hasDag) {
            handlers.sort(HANDLER_ORDER);
            return handlers.toArray(Handler.NO_HANDLERS);
        }

        List<List<Integer>> adj = new ArrayList<List<Integer>>(n);
        for (int i = 0; i < n; i++) {
            adj.add(new ArrayList<Integer>());
        }
        int[] inDegree = new int[n];

        for (int i = 0; i < n; i++) {
            Handler hi = handlers.get(i);
            Class<?> ci = hi.getListenerClass();
            for (int j = 0; j < n; j++) {
                if (i == j) continue;
                Handler hj = handlers.get(j);
                Class<?> cj = hj.getListenerClass();

                boolean mustBeBefore = false;

                if (hi.id != null && !hi.id.isEmpty() && hj.after != null) {
                    for (String a : hj.after) {
                        if (hi.id.equals(a)) {
                            mustBeBefore = true;
                            break;
                        }
                    }
                }

                if (!mustBeBefore && hj.id != null && !hj.id.isEmpty() && hi.before != null) {
                    for (String b : hi.before) {
                        if (hj.id.equals(b)) {
                            mustBeBefore = true;
                            break;
                        }
                    }
                }

                if (!mustBeBefore && ci != null && hj.afterClasses != null) {
                    for (Class<?> ac : hj.afterClasses) {
                        if (ac != null && ac.isAssignableFrom(ci)) {
                            mustBeBefore = true;
                            break;
                        }
                    }
                }

                if (!mustBeBefore && cj != null && hi.beforeClasses != null) {
                    for (Class<?> bc : hi.beforeClasses) {
                        if (bc != null && bc.isAssignableFrom(cj)) {
                            mustBeBefore = true;
                            break;
                        }
                    }
                }

                if (mustBeBefore && !adj.get(i).contains(j)) {
                    adj.get(i).add(j);
                    inDegree[j]++;
                }
            }
        }

        final List<Handler> handlerList = handlers;
        java.util.PriorityQueue<Integer> pq = new java.util.PriorityQueue<Integer>(n, new Comparator<Integer>() {
            @Override
            public int compare(Integer a, Integer b) {
                return HANDLER_ORDER.compare(handlerList.get(a), handlerList.get(b));
            }
        });

        for (int i = 0; i < n; i++) {
            if (inDegree[i] == 0) {
                pq.add(i);
            }
        }

        List<Handler> result = new ArrayList<Handler>(n);
        while (!pq.isEmpty()) {
            int u = pq.poll();
            result.add(handlers.get(u));
            for (int v : adj.get(u)) {
                inDegree[v]--;
                if (inDegree[v] == 0) {
                    pq.add(v);
                }
            }
        }

        if (result.size() < n) {
            List<String> cyclePath = findCyclePath(handlers, adj, inDegree);
            throw new CircularDependencyException("Circular dependency detected among event handlers: " + cyclePath, cyclePath);
        }

        return result.toArray(Handler.NO_HANDLERS);
    }

    private static List<String> findCyclePath(List<Handler> handlers, List<List<Integer>> adj, int[] inDegree) {
        int n = handlers.size();
        boolean[] visited = new boolean[n];
        boolean[] onStack = new boolean[n];
        List<Integer> stack = new ArrayList<Integer>();

        for (int i = 0; i < n; i++) {
            if (inDegree[i] > 0 && !visited[i]) {
                List<Integer> cycle = dfsCycle(i, adj, visited, onStack, stack);
                if (cycle != null) {
                    List<String> path = new ArrayList<String>();
                    for (int node : cycle) {
                        Handler h = handlers.get(node);
                        String name = (h.id != null && !h.id.isEmpty()) ? h.id :
                                (h.getListenerClass() != null ? h.getListenerClass().getSimpleName() : "Handler@" + h.order);
                        path.add(name);
                    }
                    return path;
                }
            }
        }
        return java.util.Collections.emptyList();
    }

    private static List<Integer> dfsCycle(int u, List<List<Integer>> adj, boolean[] visited, boolean[] onStack, List<Integer> stack) {
        visited[u] = true;
        onStack[u] = true;
        stack.add(u);

        for (int v : adj.get(u)) {
            if (!visited[v]) {
                List<Integer> cycle = dfsCycle(v, adj, visited, onStack, stack);
                if (cycle != null) return cycle;
            } else if (onStack[v]) {
                List<Integer> cycle = new ArrayList<Integer>();
                int startIdx = stack.indexOf(v);
                for (int k = startIdx; k < stack.size(); k++) {
                    cycle.add(stack.get(k));
                }
                cycle.add(v);
                return cycle;
            }
        }

        onStack[u] = false;
        stack.remove(stack.size() - 1);
        return null;
    }
}
