package com.jdbcagent.server.jdbc;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * 定期清理过期的语句和结果集缓存，触发关闭底层游标。
 */
final class ServerCacheMaintenance {
    private static final ScheduledExecutorService EXECUTOR =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
                @Override
                public Thread newThread(Runnable runnable) {
                    Thread thread = new Thread(runnable, "jdbc-agent-cache-cleaner");
                    thread.setDaemon(true);
                    return thread;
                }
            });

    static {
        EXECUTOR.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                StatementServer.STATEMENTS.cleanUp();
                ResultSetServer.RESULTSETS.cleanUp();
            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    private ServerCacheMaintenance() {
    }

    static void start() {
    }
}
