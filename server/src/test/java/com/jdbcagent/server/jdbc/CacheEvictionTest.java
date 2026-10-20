package com.jdbcagent.server.jdbc;

import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

public class CacheEvictionTest {

    @Test
    public void expiredStatementIsClosed() throws Exception {
        ManualTicker ticker = new ManualTicker();
        Cache<Long, StatementServer> cache = CacheBuilder.newBuilder()
                .expireAfterAccess(1, TimeUnit.MILLISECONDS)
                .ticker(ticker)
                .removalListener(StatementServer.CLOSE_ON_REMOVAL)
                .build();
        final int[] closes = new int[1];
        StatementServer server = new StatementServer(statement(closes));
        try {
            cache.put(server.currentId, server);
            ticker.advanceMillis(5);
            cache.cleanUp();
            Assert.assertNull(cache.getIfPresent(server.currentId));
            Assert.assertEquals(1, closes[0]);
        } finally {
            StatementServer.STATEMENTS.invalidate(server.currentId);
        }
    }

    @Test
    public void closeStatementClosesOnce() throws Exception {
        final int[] closes = new int[1];
        StatementServer server = new StatementServer(statement(closes));
        server.close();
        Assert.assertEquals(1, closes[0]);
        Assert.assertNull(StatementServer.STATEMENTS.getIfPresent(server.currentId));
    }

    @Test
    public void expiredResultSetIsClosed() throws Exception {
        ManualTicker ticker = new ManualTicker();
        Cache<Long, ResultSetServer> cache = CacheBuilder.newBuilder()
                .expireAfterAccess(1, TimeUnit.MILLISECONDS)
                .ticker(ticker)
                .removalListener(ResultSetServer.CLOSE_ON_REMOVAL)
                .build();
        final int[] closes = new int[1];
        ResultSetServer server = new ResultSetServer(resultSet(closes));
        try {
            cache.put(server.currentId, server);
            ticker.advanceMillis(5);
            cache.cleanUp();
            Assert.assertNull(cache.getIfPresent(server.currentId));
            Assert.assertEquals(1, closes[0]);
        } finally {
            ResultSetServer.RESULTSETS.invalidate(server.currentId);
        }
    }

    @Test
    public void closeResultSetClosesOnce() throws Exception {
        final int[] closes = new int[1];
        ResultSetServer server = new ResultSetServer(resultSet(closes));
        server.close();
        Assert.assertEquals(1, closes[0]);
        Assert.assertNull(ResultSetServer.RESULTSETS.getIfPresent(server.currentId));
    }

    private static Statement statement(final int[] closes) {
        return (Statement) Proxy.newProxyInstance(CacheEvictionTest.class.getClassLoader(),
                new Class<?>[]{Statement.class}, new CloseHandler(closes));
    }

    private static ResultSet resultSet(final int[] closes) {
        return (ResultSet) Proxy.newProxyInstance(CacheEvictionTest.class.getClassLoader(),
                new Class<?>[]{ResultSet.class}, new CloseHandler(closes));
    }

    private static final class CloseHandler implements InvocationHandler {
        private final int[] closes;
        private boolean closed;

        private CloseHandler(int[] closes) {
            this.closes = closes;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            String name = method.getName();
            if ("close".equals(name)) {
                closes[0]++;
                closed = true;
                return null;
            }
            if ("isClosed".equals(name)) {
                return closed;
            }
            if ("hashCode".equals(name)) {
                return System.identityHashCode(proxy);
            }
            if ("equals".equals(name)) {
                return proxy == args[0];
            }
            if ("toString".equals(name)) {
                return "proxy";
            }
            throw new UnsupportedOperationException(name);
        }
    }

    private static final class ManualTicker extends Ticker {
        private long nanos;

        @Override
        public long read() {
            return nanos;
        }

        void advanceMillis(long millis) {
            nanos += TimeUnit.MILLISECONDS.toNanos(millis);
        }
    }
}
