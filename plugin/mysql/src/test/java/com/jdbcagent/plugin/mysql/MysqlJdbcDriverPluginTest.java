package com.jdbcagent.plugin.mysql;

import org.junit.Assert;
import org.junit.Test;

import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Enumeration;

public class MysqlJdbcDriverPluginTest {

    @Test
    public void loadsMysqlDriversAndReleasesThem() throws Exception {
        MysqlJdbcDriverPlugin plugin = new MysqlJdbcDriverPlugin();
        Assert.assertEquals("mysql", plugin.name());

        Driver driver = plugin.loadDriver("com.mysql.cj.jdbc.Driver");
        Assert.assertTrue(driver.acceptsURL("jdbc:mysql://127.0.0.1:3306/mytest"));
        Assert.assertFalse(driver.acceptsURL("jdbc:h2:mem:test"));

        Driver legacy = plugin.loadDriver("com.mysql.jdbc.Driver");
        Assert.assertTrue(legacy.acceptsURL("jdbc:mysql://127.0.0.1:3306/mytest"));
        Assert.assertFalse(legacy.acceptsURL("jdbc:h2:mem:test"));
        // 8.x 的 com.mysql.jdbc.Driver 只打印弃用提示，真正登记到 DriverManager 的是 cj 驱动
        Assert.assertTrue(visible("com.mysql.cj.jdbc.Driver"));
        Assert.assertFalse(visible("com.mysql.jdbc.Driver"));

        plugin.release();
        Assert.assertFalse(visible("com.mysql.cj.jdbc.Driver"));
        Assert.assertFalse(visible("com.mysql.jdbc.Driver"));
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
