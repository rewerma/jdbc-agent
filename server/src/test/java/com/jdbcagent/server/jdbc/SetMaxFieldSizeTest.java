package com.jdbcagent.server.jdbc;

import com.jdbcagent.core.protocol.ConnectionMsg;
import com.jdbcagent.core.protocol.StatementMsg;
import com.jdbcagent.server.config.JdbcAgentConf;
import org.junit.Assert;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

public class SetMaxFieldSizeTest {

    @Test
    public void setMaxFieldSizeCallsSetMaxFieldSize() throws Exception {
        Class.forName("org.h2.Driver");
        final Connection real = DriverManager.getConnection(
                "jdbc:h2:mem:set_max_field_size;DB_CLOSE_DELAY=-1", "sa", "");
        final String[] called = new String[1];
        final int[] value = new int[1];
        final Statement realStatement = real.createStatement();
        final Statement statement = (Statement) Proxy.newProxyInstance(
                Statement.class.getClassLoader(),
                new Class<?>[]{Statement.class},
                new InvocationHandler() {
                    @Override
                    public Object invoke(Object proxy, Method method, Object[] methodArgs) throws Throwable {
                        if ("setMaxFieldSize".equals(method.getName()) || "setMaxRows".equals(method.getName())) {
                            called[0] = method.getName();
                            value[0] = (Integer) methodArgs[0];
                        }
                        return delegate(realStatement, method, methodArgs);
                    }
                });
        Connection connection = (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                new InvocationHandler() {
                    @Override
                    public Object invoke(Object proxy, Method method, Object[] methodArgs) throws Throwable {
                        if ("createStatement".equals(method.getName())
                                && (methodArgs == null || methodArgs.length == 0)) {
                            return statement;
                        }
                        return delegate(real, method, methodArgs);
                    }
                });
        putDataSource("field_db", "field_user", "field_pass", dataSource(connection));

        ConnectionServer connectionServer = new ConnectionServer();
        connectionServer.connect(8, "field_db", "field_user", "field_pass");
        try {
            Long statementId = (Long) connectionServer.connMethod(ConnectionMsg.newBuilder()
                    .setMethod(ConnectionMsg.Method.createStatement)
                    .setParams(new Serializable[0])
                    .build());
            StatementServer statementServer = StatementServer.STATEMENTS.getIfPresent(statementId);
            statementServer.statementMethod(StatementMsg.newBuilder()
                    .setMethod(StatementMsg.Method.setMaxFieldSize)
                    .setParams(new Serializable[]{12})
                    .build());
            Assert.assertEquals("setMaxFieldSize", called[0]);
            Assert.assertEquals(12, value[0]);
            statementServer.close();
        } finally {
            connectionServer.connMethod(ConnectionMsg.newBuilder().setMethod(ConnectionMsg.Method.release).build());
        }
    }

    private static Object delegate(Object target, Method method, Object[] methodArgs) throws Throwable {
        try {
            return method.invoke(target, methodArgs);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private static DataSource dataSource(final Connection connection) {
        return new DataSource() {
            @Override
            public Connection getConnection() {
                return connection;
            }

            @Override
            public Connection getConnection(String username, String password) {
                throw new UnsupportedOperationException();
            }

            @Override
            public java.io.PrintWriter getLogWriter() {
                return null;
            }

            @Override
            public void setLogWriter(java.io.PrintWriter out) {
            }

            @Override
            public void setLoginTimeout(int seconds) {
            }

            @Override
            public int getLoginTimeout() {
                return 0;
            }

            @Override
            public java.util.logging.Logger getParentLogger() {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> T unwrap(Class<T> iface) throws SQLException {
                throw new SQLException();
            }

            @Override
            public boolean isWrapperFor(Class<?> iface) {
                return false;
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static void putDataSource(String catalog, String username, String password, DataSource dataSource)
            throws Exception {
        Field sources = JdbcAgentConf.class.getDeclaredField("DATASOURCE_MAP");
        sources.setAccessible(true);
        ((Map<JdbcAgentConf.DataSourceKey, DataSource>) sources.get(null))
                .put(new JdbcAgentConf.DataSourceKey(catalog, username), dataSource);
        Field passwords = JdbcAgentConf.class.getDeclaredField("ACCESS_PASSWORDS");
        passwords.setAccessible(true);
        ((Map<String, String>) passwords.get(null)).put(username, password);
    }
}
