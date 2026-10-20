package com.jdbcagent.server.netty;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 在独立的有界线程池里执行 JDBC，避免慢查询占住 Netty 的 IO 线程。
 */
public final class JdbcDispatch {
    static final int QUEUE_CAPACITY = 1024;

    private static volatile ThreadPoolExecutor workers;

    private JdbcDispatch() {
    }

    public static void execute(Runnable task) {
        workers().execute(task);
    }

    public static void shutdown() {
        ThreadPoolExecutor current;
        synchronized (JdbcDispatch.class) {
            current = workers;
            workers = null;
        }
        if (current == null) {
            return;
        }
        current.shutdown();
        try {
            if (!current.awaitTermination(5, TimeUnit.SECONDS)) {
                current.shutdownNow();
            }
        } catch (InterruptedException e) {
            current.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    static ThreadPoolExecutor newExecutor(int threads, int queueCapacity) {
        return new ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(queueCapacity),
                namedThreads("jdbc-agent-jdbc-"),
                new ThreadPoolExecutor.AbortPolicy());
    }

    static int threads() {
        return Math.max(4, Runtime.getRuntime().availableProcessors() * 2);
    }

    private static ThreadPoolExecutor workers() {
        ThreadPoolExecutor current = workers;
        if (current != null && !current.isShutdown()) {
            return current;
        }
        synchronized (JdbcDispatch.class) {
            if (workers == null || workers.isShutdown()) {
                workers = newExecutor(threads(), QUEUE_CAPACITY);
            }
            return workers;
        }
    }

    static ThreadFactory namedThreads(String prefix) {
        return new ThreadFactory() {
            private final AtomicInteger index = new AtomicInteger();

            @Override
            public Thread newThread(Runnable runnable) {
                Thread thread = new Thread(runnable, prefix + index.incrementAndGet());
                thread.setDaemon(false);
                return thread;
            }
        };
    }
}
