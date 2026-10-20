package com.jdbcagent.server.netty;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Netty 4 的 ChannelId 不是整数。连接表仍按通道保存一个进程内唯一的整数。
 */
public final class ChannelIds {
    private static final AttributeKey<Integer> KEY = AttributeKey.valueOf("jdbcAgentChannelId");
    private static final AtomicInteger NEXT = new AtomicInteger();

    private ChannelIds() {
    }

    public static Integer id(Channel channel) {
        Integer existing = channel.attr(KEY).get();
        if (existing != null) {
            return existing;
        }
        Integer created = Integer.valueOf(NEXT.incrementAndGet());
        Integer raced = channel.attr(KEY).setIfAbsent(created);
        return raced == null ? created : raced;
    }
}
