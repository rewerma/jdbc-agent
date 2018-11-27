package com.jdbcagent.client.util.loadbalance;

import com.jdbcagent.core.util.ServerRunningData;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinLoadBalance {
    public static final String NAME = "roundrobin";
    private static final ConcurrentMap<String, AtomicInteger> sequences = new ConcurrentHashMap<String, AtomicInteger>();
    private static final ConcurrentMap<String, AtomicInteger> weightSequences = new ConcurrentHashMap<String, AtomicInteger>();

    public static ServerRunningData doSelect(List<ServerRunningData> serverRunningDataList) {
        String key = serverRunningDataList.get(0).getCatalog();
        int length = serverRunningDataList.size(); // 总个数
        int maxWeight = 0; // 最大权重
        int minWeight = Integer.MAX_VALUE; // 最小权重
        for (int i = 0; i < length; i++) {
            int weight = serverRunningDataList.get(i).getWeight();
            maxWeight = Math.max(maxWeight, weight); // 累计最大权重
            minWeight = Math.min(minWeight, weight); // 累计最小权重
        }
        if (maxWeight > 0 && minWeight < maxWeight) { // 权重不一样
            AtomicInteger weightSequence = weightSequences.get(key);
            if (weightSequence == null) {
                weightSequences.putIfAbsent(key, new AtomicInteger());
                weightSequence = weightSequences.get(key);
            }
            int currentWeight = weightSequence.getAndIncrement() % maxWeight;
            List<ServerRunningData> weightInvokers = new ArrayList<ServerRunningData>();
            for (ServerRunningData serverRunningData : serverRunningDataList) { // 筛选权重大于当前权重基数的Invoker
                if (serverRunningData.getWeight() > currentWeight) {
                    weightInvokers.add(serverRunningData);
                }
            }
            int weightLength = weightInvokers.size();
            if (weightLength == 1) {
                return weightInvokers.get(0);
            } else if (weightLength > 1) {
                serverRunningDataList = weightInvokers;
                length = serverRunningDataList.size();
            }
        }
        AtomicInteger sequence = sequences.get(key);
        if (sequence == null) {
            sequences.putIfAbsent(key, new AtomicInteger());
            sequence = sequences.get(key);
        }
        // 取模轮循
        return serverRunningDataList.get(Math.abs(sequence.getAndIncrement() % length));
    }
}
