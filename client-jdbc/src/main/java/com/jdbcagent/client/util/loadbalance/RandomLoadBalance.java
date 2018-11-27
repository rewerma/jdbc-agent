package com.jdbcagent.client.util.loadbalance;

import com.jdbcagent.core.util.ServerRunningData;

import java.util.List;
import java.util.Random;

public class RandomLoadBalance {
    public static final String NAME = "random";
    private static final Random random = new Random();

    public static ServerRunningData doSelect(List<ServerRunningData> serverRunningDataList) {
        int length = serverRunningDataList.size(); // 总个数
        int totalWeight = 0; // 总权重
        boolean sameWeight = true; // 权重是否都一样
        for (int i = 0; i < length; i++) {
            int weight = serverRunningDataList.get(i).getWeight();
            totalWeight += weight; // 累计总权重
            if (sameWeight && i > 0
                    && weight != serverRunningDataList.get(i - 1).getWeight()) {
                sameWeight = false; // 计算所有权重是否一样
            }
        }
        if (totalWeight > 0 && !sameWeight) {
            // 如果权重不相同且权重大于0则按总权重数随机
            int offset = random.nextInt(totalWeight);
            // 并确定随机值落在哪个片断上
            for (int i = 0; i < length; i++) {
                offset -= serverRunningDataList.get(i).getWeight();
                if (offset < 0) {
                    return serverRunningDataList.get(i);
                }
            }
        }
        // 如果权重相同或权重为0则均等随机
        return serverRunningDataList.get(random.nextInt(length));
    }
}
