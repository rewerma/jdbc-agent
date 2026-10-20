package com.jdbcagent.plugin;

import java.sql.Driver;
import java.sql.SQLException;

/**
 * 一个 JDBC 驱动插件。每个插件由 server 用单独的类加载器装入，实现类不要把卸载逻辑委托给
 * 父类加载器里的方法，否则 DriverManager 认不出调用方，卸不掉驱动。
 */
public interface JdbcDriverPlugin {

    /**
     * 插件名，写进日志，用来区分是哪一个插件提供了驱动。
     */
    String name();

    /**
     * 这个插件能装的驱动类名，例如 com.mysql.cj.jdbc.Driver。
     */
    String[] driverClassNames();

    /**
     * 用本插件的类加载器创建驱动。返回的实例由 server 再包一层后注册到 DriverManager。
     */
    Driver loadDriver(String driverClassName) throws SQLException;

    /**
     * 卸掉本插件注册进 DriverManager 的驱动。必须在实现类里直接调用 DriverManager.deregisterDriver。
     */
    void release() throws SQLException;
}
