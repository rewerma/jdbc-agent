package com.jdbcagent.server.datasources;

import com.jdbcagent.core.support.datasource.AbstractDataSource;
import com.jdbcagent.server.config.DataSourceConf;
import org.junit.Assert;
import org.junit.Test;

import javax.sql.DataSource;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class LimitedDataSourceTest {

    @Test
    public void rejectWhenActiveConnectionsReachMax() throws Exception {
        LimitedDataSource dataSource = new LimitedDataSource(new StubDataSource(), 1);
        Connection first = dataSource.getConnection();

        try {
            dataSource.getConnection();
            Assert.fail("should reject the second connection");
        } catch (SQLException e) {
            Assert.assertEquals("08004", e.getSQLState());
            Assert.assertTrue(e.getMessage().contains("maxConnections=1"));
        }

        first.close();
        Connection second = dataSource.getConnection();
        second.close();
    }

    @Test
    public void closeReleasesOnlyOnce() throws Exception {
        LimitedDataSource dataSource = new LimitedDataSource(new StubDataSource(), 1);
        Connection connection = dataSource.getConnection();
        connection.close();
        connection.close();

        Connection another = dataSource.getConnection();
        another.close();
    }

    @Test
    public void alreadyClosedConnectionReleasesSlot() throws Exception {
        StubDataSource stub = new StubDataSource();
        stub.closedOnCreate = true;
        LimitedDataSource dataSource = new LimitedDataSource(stub, 1);
        Connection connection = dataSource.getConnection();
        Assert.assertTrue(connection.isClosed());

        Connection another = dataSource.getConnection();
        another.close();
    }

    @Test
    public void failedOpenDoesNotConsumeSlot() throws Exception {
        StubDataSource stub = new StubDataSource();
        stub.failure = new SQLException("open failed");
        LimitedDataSource dataSource = new LimitedDataSource(stub, 1);

        try {
            dataSource.getConnection();
            Assert.fail("open should fail");
        } catch (SQLException e) {
            Assert.assertEquals("open failed", e.getMessage());
        }

        Connection connection = dataSource.getConnection();
        connection.close();
    }

    @Test
    public void factoryAppliesMaxConnections() {
        DataSourceConf conf = new DataSourceConf();
        Map<String, Object> dsManager = new HashMap<String, Object>();
        dsManager.put("driverClass", "org.h2.Driver");
        dsManager.put("jdbcUrl", "jdbc:h2:mem:limit");
        dsManager.put("username", "sa");
        dsManager.put("password", "");
        conf.setDsManager(dsManager);
        conf.setMaxConnections(2);

        DataSource dataSource = DataSourceFactory.getDataSource(conf);
        Assert.assertTrue(dataSource instanceof LimitedDataSource);

        conf.setMaxConnections(null);
        DataSource unlimited = DataSourceFactory.getDataSource(conf);
        Assert.assertFalse(unlimited instanceof LimitedDataSource);
    }

    @Test
    public void negativeMaxConnectionsIsRejected() {
        DataSourceConf conf = new DataSourceConf();
        Map<String, Object> dsManager = new HashMap<String, Object>();
        dsManager.put("driverClass", "org.h2.Driver");
        dsManager.put("jdbcUrl", "jdbc:h2:mem:limit");
        dsManager.put("username", "sa");
        dsManager.put("password", "");
        conf.setDsManager(dsManager);
        conf.setMaxConnections(-1);

        try {
            DataSourceFactory.getDataSource(conf);
            Assert.fail("negative maxConnections should be rejected");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("maxConnections"));
        }
    }

    private static final class StubDataSource extends AbstractDataSource {
        private SQLException failure;
        private boolean closedOnCreate;

        @Override
        public Connection getConnection() throws SQLException {
            if (failure != null) {
                SQLException ex = failure;
                failure = null;
                throw ex;
            }
            return newStubConnection(closedOnCreate);
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            return getConnection();
        }
    }

    private static Connection newStubConnection(final boolean closedOnCreate) {
        final AtomicBoolean closed = new AtomicBoolean(closedOnCreate);
        ClassLoader classLoader = Connection.class.getClassLoader();
        if (classLoader == null) {
            classLoader = LimitedDataSourceTest.class.getClassLoader();
        }
        return (Connection) Proxy.newProxyInstance(classLoader, new Class<?>[]{Connection.class},
                new InvocationHandler() {
                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args) {
                        String name = method.getName();
                        if ("close".equals(name)) {
                            closed.set(true);
                            return null;
                        }
                        if ("isClosed".equals(name)) {
                            return closed.get();
                        }
                        Class<?> returnType = method.getReturnType();
                        if (returnType == boolean.class) {
                            return false;
                        }
                        if (returnType == int.class || returnType == short.class || returnType == byte.class) {
                            return 0;
                        }
                        if (returnType == long.class) {
                            return 0L;
                        }
                        if (returnType == float.class) {
                            return 0F;
                        }
                        if (returnType == double.class) {
                            return 0D;
                        }
                        return null;
                    }
                });
    }
}
