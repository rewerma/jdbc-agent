package com.jdbcagent.server.jdbc;

import com.jdbcagent.core.protocol.ConnectionMsg;
import com.jdbcagent.server.config.JdbcAgentConf;
import org.junit.Assert;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class CreateStatementTest {

    @Test
    public void threeArgCreateStatementReadsHoldability() throws Exception {
        Class.forName("org.h2.Driver");
        final Connection real = DriverManager.getConnection(
                "jdbc:h2:mem:create_statement;DB_CLOSE_DELAY=-1", "sa", "");
        final int[] args = new int[3];
        Connection connection = (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                new InvocationHandler() {
                    @Override
                    public Object invoke(Object proxy, Method method, Object[] methodArgs) throws Throwable {
                        if ("createStatement".equals(method.getName()) && methodArgs != null && methodArgs.length == 3) {
                            args[0] = (Integer) methodArgs[0];
                            args[1] = (Integer) methodArgs[1];
                            args[2] = (Integer) methodArgs[2];
                        }
                        try {
                            return method.invoke(real, methodArgs);
                        } catch (java.lang.reflect.InvocationTargetException e) {
                            throw e.getCause();
                        }
                    }
                });
        DataSource dataSource = new DataSource() {
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
        putDataSource("stmt_db", "stmt_user", "stmt_pass", dataSource);

        ConnectionServer connectionServer = new ConnectionServer();
        connectionServer.connect(7, "stmt_db", "stmt_user", "stmt_pass");
        try {
            Serializable statementId = connectionServer.connMethod(ConnectionMsg.newBuilder()
                    .setMethod(ConnectionMsg.Method.createStatement)
                    .setParams(new Serializable[]{
                            ResultSet.TYPE_SCROLL_INSENSITIVE,
                            ResultSet.CONCUR_READ_ONLY,
                            ResultSet.CLOSE_CURSORS_AT_COMMIT
                    })
                    .build());
            Assert.assertNotNull(statementId);
            Assert.assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, args[0]);
            Assert.assertEquals(ResultSet.CONCUR_READ_ONLY, args[1]);
            Assert.assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, args[2]);
            StatementServer.STATEMENTS.getIfPresent((Long) statementId).close();
        } finally {
            connectionServer.connMethod(ConnectionMsg.newBuilder().setMethod(ConnectionMsg.Method.release).build());
        }
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
