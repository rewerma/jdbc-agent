package com.jdbcagent.server.jdbc;

/**
 * 当前 Netty 工作线程正在处理的通道。结果集创建时用来登记清理关系。
 */
public final class DispatchChannels {
    private static final ThreadLocal<Integer> CURRENT = new ThreadLocal<Integer>();

    private DispatchChannels() {
    }

    public static void set(Integer channelId) {
        CURRENT.set(channelId);
    }

    public static void clear() {
        CURRENT.remove();
    }

    static Integer get() {
        return CURRENT.get();
    }
}
