package com.jdbcagent.plugin.mysql;

import com.jdbcagent.plugin.JdbcDriverPlugin;

import java.lang.reflect.InvocationTargetException;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * MySQL Connector/J 插件。驱动类留在本插件的类加载器里，不放进 server。
 */
public class MysqlJdbcDriverPlugin implements JdbcDriverPlugin {

    @Override
    public String name() {
        return "mysql";
    }

    @Override
    public String[] driverClassNames() {
        return new String[]{
                "com.mysql.cj.jdbc.Driver",
                "com.mysql.jdbc.Driver"
        };
    }

    @Override
    public Driver loadDriver(String driverClassName) throws SQLException {
        String name = driverClassName == null ? "" : driverClassName.trim();
        if (!declares(name)) {
            throw new SQLException("plugin mysql does not provide " + driverClassName);
        }
        try {
            Class<?> type = Class.forName(name, true, MysqlJdbcDriverPlugin.class.getClassLoader());
            Object instance = type.getDeclaredConstructor().newInstance();
            if (!(instance instanceof Driver)) {
                throw new SQLException(name + " is not a java.sql.Driver");
            }
            return (Driver) instance;
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            if (cause instanceof SQLException) {
                throw (SQLException) cause;
            }
            throw new SQLException("failed to load JDBC driver " + name, cause);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException("failed to load JDBC driver " + name, e);
        }
    }

    @Override
    public void release() throws SQLException {
        // 调用方必须是本类。委托给父类加载器里的工具方法时，DriverManager 会拒绝卸载。
        Set<String> names = new HashSet<String>();
        String[] declared = driverClassNames();
        if (declared != null) {
            for (String driverClassName : declared) {
                if (driverClassName != null) {
                    names.add(driverClassName);
                }
            }
        }
        ClassLoader mine = MysqlJdbcDriverPlugin.class.getClassLoader();
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        List<Driver> owned = new ArrayList<Driver>();
        while (drivers.hasMoreElements()) {
            Driver driver = drivers.nextElement();
            if (names.contains(driver.getClass().getName())
                    && driver.getClass().getClassLoader() == mine) {
                owned.add(driver);
            }
        }
        for (Driver driver : owned) {
            DriverManager.deregisterDriver(driver);
        }
    }

    private boolean declares(String driverClassName) {
        String[] names = driverClassNames();
        if (names == null) {
            return false;
        }
        for (String name : names) {
            if (driverClassName.equals(name)) {
                return true;
            }
        }
        return false;
    }
}
