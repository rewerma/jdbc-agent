package com.jdbcagent.plugin.postgresql;

import org.junit.Assert;
import org.junit.Test;

import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Enumeration;

public class PostgresqlJdbcDriverPluginTest {

    @Test
    public void loadsPostgresqlDriverAndReleasesIt() throws Exception {
        PostgresqlJdbcDriverPlugin plugin = new PostgresqlJdbcDriverPlugin();
        Assert.assertEquals("postgresql", plugin.name());

        Driver driver = plugin.loadDriver("org.postgresql.Driver");
        Assert.assertTrue(driver.acceptsURL("jdbc:postgresql://127.0.0.1:5432/mytest"));
        Assert.assertFalse(driver.acceptsURL("jdbc:mysql://127.0.0.1:3306/mytest"));
        Assert.assertTrue(visible("org.postgresql.Driver"));

        plugin.release();
        Assert.assertFalse(visible("org.postgresql.Driver"));
    }

    private static boolean visible(String className) {
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            if (className.equals(drivers.nextElement().getClass().getName())) {
                return true;
            }
        }
        return false;
    }
}
