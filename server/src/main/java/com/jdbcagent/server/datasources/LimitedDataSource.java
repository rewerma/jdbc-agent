package com.jdbcagent.server.datasources;

import com.jdbcagent.core.support.datasource.AbstractDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 限制单个数据源同时持有的 JDBC 连接数。达到上限后拒绝新建连接，连接关闭后释放名额。
 */
public class LimitedDataSource extends AbstractDataSource {
    private static final Logger logger = LoggerFactory.getLogger(LimitedDataSource.class);
    private static final String TOO_MANY_CONNECTIONS_STATE = "08004";

    private final DataSource target;
    private final int maxConnections;
    private final AtomicInteger activeCount = new AtomicInteger(0);

    public LimitedDataSource(DataSource target, int maxConnections) {
        this.target = target;
        this.maxConnections = maxConnections;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return open(new ConnectionOpener() {
            @Override
            public Connection open() throws SQLException {
                return target.getConnection();
            }
        });
    }

    @Override
    public Connection getConnection(final String username, final String password) throws SQLException {
        return open(new ConnectionOpener() {
            @Override
            public Connection open() throws SQLException {
                return target.getConnection(username, password);
            }
        });
    }

    private Connection open(ConnectionOpener opener) throws SQLException {
        acquire();
        Connection raw = null;
        try {
            raw = opener.open();
            return wrap(raw);
        } catch (Throwable ex) {
            release();
            if (raw != null) {
                try {
                    raw.close();
                } catch (SQLException ignored) {
                    // 建连失败时尽力关闭已创建的物理连接
                }
            }
            if (ex instanceof SQLException) {
                throw (SQLException) ex;
            }
            if (ex instanceof RuntimeException) {
                throw (RuntimeException) ex;
            }
            if (ex instanceof Error) {
                throw (Error) ex;
            }
            throw new SQLException(ex);
        }
    }

    private void acquire() throws SQLException {
        while (true) {
            int current = activeCount.get();
            if (current >= maxConnections) {
                logger.warn("Reject new JDBC connection, active={}, maxConnections={}", current, maxConnections);
                throw new SQLException("Too many connections, maxConnections=" + maxConnections,
                        TOO_MANY_CONNECTIONS_STATE);
            }
            if (activeCount.compareAndSet(current, current + 1)) {
                return;
            }
        }
    }

    private void release() {
        while (true) {
            int current = activeCount.get();
            if (current <= 0) {
                return;
            }
            if (activeCount.compareAndSet(current, current - 1)) {
                return;
            }
        }
    }

    private Connection wrap(Connection raw) {
        ClassLoader classLoader = Connection.class.getClassLoader();
        if (classLoader == null) {
            classLoader = LimitedDataSource.class.getClassLoader();
        }
        return (Connection) Proxy.newProxyInstance(classLoader, new Class<?>[]{Connection.class},
                new CloseTrackingHandler(raw));
    }

    private interface ConnectionOpener {
        Connection open() throws SQLException;
    }

    private final class CloseTrackingHandler implements InvocationHandler {
        private final Connection delegate;
        private final AtomicBoolean released = new AtomicBoolean(false);

        private CloseTrackingHandler(Connection delegate) {
            this.delegate = delegate;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String name = method.getName();
            if ("close".equals(name) && (args == null || args.length == 0)) {
                try {
                    return invokeDelegate(method, args);
                } finally {
                    releaseOnce();
                }
            }
            if ("isClosed".equals(name) && (args == null || args.length == 0)) {
                Object closed = invokeDelegate(method, args);
                if (Boolean.TRUE.equals(closed)) {
                    releaseOnce();
                }
                return closed;
            }
            if ("equals".equals(name) && args != null && args.length == 1) {
                return proxy == args[0];
            }
            if ("hashCode".equals(name) && (args == null || args.length == 0)) {
                return System.identityHashCode(proxy);
            }
            return invokeDelegate(method, args);
        }

        private Object invokeDelegate(Method method, Object[] args) throws Throwable {
            try {
                return method.invoke(delegate, args);
            } catch (InvocationTargetException e) {
                Throwable cause = e.getCause();
                throw cause != null ? cause : e;
            }
        }

        private void releaseOnce() {
            if (released.compareAndSet(false, true)) {
                release();
            }
        }
    }
}
