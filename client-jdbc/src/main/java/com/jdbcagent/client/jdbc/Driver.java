package com.jdbcagent.client.jdbc;

import com.jdbcagent.client.util.Util;

import java.net.InetSocketAddress;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * JDBC-Agent client jdbc driver
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class Driver implements java.sql.Driver {

    private static final Driver INSTANCE = new Driver();

    private static volatile boolean registered;

    static {
        load();
    }

    /**
     * Open a database connection. This method should not be called by an application. Instead, the
     * method DriverManager.getConnection should be used.
     *
     * @param url  the database URL
     * @param info the connection properties
     * @return the new connection or null if the URL is not supported
     */
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        try {
            if (info == null) {
                info = new Properties();
            }
            if (!acceptsURL(url)) {
                return null;
            }
            Map<String, String> urlInfo = Util.parseUrl(url);
            int idleTimeout = Integer.parseInt(info.getProperty("timeout", "1800000"));

            JdbcAgentRpcClient jdbcAgentConnector = new JdbcAgentRpcClient(
                    new InetSocketAddress(urlInfo.get("ip"), Integer.parseInt(urlInfo.get("port"))), idleTimeout);
            jdbcAgentConnector.connect();

            String catalog = urlInfo.get("catalog");
            String username = info.getProperty("user");
            String password = info.getProperty("password");

            return new JdbcConnection(jdbcAgentConnector, catalog, username, password);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    /**
     * Check if the driver understands this URL. This method should not be called by an application.
     *
     * @param url the database URL
     * @return if the driver understands the URL
     */
    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if (url != null) {
            if (url.startsWith("jdbc:agent:")) {
                return true;
            } else if (url.startsWith("jdbc:zookeeper:")) {
                return true;
            }
        }
        return false;
    }

    /**
     * Get the list of supported properties. This method should not be called by an application.
     *
     * @param url  the database URL
     * @param info the connection properties
     * @return a zero length array
     */
    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    /**
     * Get the major version number of the driver. This method should not be called by an
     * application.
     *
     * @return the major version number
     */
    @Override
    public int getMajorVersion() {
        return 1;
    }

    /**
     * Get the minor version number of the driver. This method should not be called by an
     * application.
     *
     * @return the minor version number
     */
    @Override
    public int getMinorVersion() {
        return 4;
    }

    /**
     * Check if this driver is compliant to the JDBC specification. This method should not be called
     * by an application.
     *
     * @return true
     */
    @Override
    public boolean jdbcCompliant() {
        return true;
    }

    /**
     * [Not supported]
     */
    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

    /**
     * INTERNAL
     */
    public static synchronized Driver load() {
        try {
            if (!registered) {
                registered = true;
                DriverManager.registerDriver(INSTANCE);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return INSTANCE;
    }

    /**
     * INTERNAL
     */
    public static synchronized void unload() {
        try {
            if (registered) {
                registered = false;
                DriverManager.deregisterDriver(INSTANCE);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
