package com.jdbcagent.client.uitl;

import com.jdbcagent.core.util.ClusterNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 在存活成员里选一台。负载是连接数除以权重，相同负载按轮询打散。
 */
public final class ClusterPicker {
    private static final AtomicInteger ROTATION = new AtomicInteger();

    private ClusterPicker() {
    }

    public static ClusterNode pick(List<ClusterNode> members, Set<String> excluded) {
        if (members == null || members.isEmpty()) {
            return null;
        }
        Set<String> skip = excluded == null ? Collections.<String>emptySet() : excluded;
        List<ClusterNode> chosen = new ArrayList<ClusterNode>();
        long best = Long.MAX_VALUE;
        for (int i = 0; i < members.size(); i++) {
            ClusterNode node = members.get(i);
            if (node == null || node.getAddress() == null || node.getAddress().length() == 0) {
                continue;
            }
            if (skip.contains(node.getAddress())) {
                continue;
            }
            int weight = node.getWeight() < 1 ? 1 : node.getWeight();
            int active = node.getActiveConnections() < 0 ? 0 : node.getActiveConnections();
            long load = (long) active * 1000L / weight;
            if (load < best) {
                best = load;
                chosen.clear();
                chosen.add(node);
            } else if (load == best) {
                chosen.add(node);
            }
        }
        if (chosen.isEmpty()) {
            return null;
        }
        int index = Math.floorMod(ROTATION.getAndIncrement(), chosen.size());
        return chosen.get(index);
    }
}
