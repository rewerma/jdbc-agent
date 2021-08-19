package com.jdbcagent.core.support.datasource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DriverManagerDataSource extends AbstractDriverBasedDataSource {
    private static Logger logger = LoggerFactory.getLogger(DriverManagerDataSource.class);

    public DriverManagerDataSource() {
    }

    public DriverManagerDataSource(String url) {
        this.setUrl(url);
    }

    public DriverManagerDataSource(String url, String username, String password) {
        this.setUrl(url);
        this.setUsername(username);
        this.setPassword(password);
    }

    public DriverManagerDataSource(String url, Properties conProps) {
        this.setUrl(url);
        this.setConnectionProperties(conProps);
    }

    /**
     * @deprecated
     */
    @Deprecated
    public DriverManagerDataSource(String driverClassName, String url, String username, String password) {
        this.setDriverClassName(driverClassName);
        this.setUrl(url);
        this.setUsername(username);
        this.setPassword(password);
    }

    public void setDriverClassName(String driverClassName) {
        // Assert.hasText(driverClassName, "Property 'driverClassName' must not be empty");
        String driverClassNameToUse = driverClassName.trim();

        try {
            Class.forName(driverClassNameToUse, true, getDefaultClassLoader());
        } catch (ClassNotFoundException var4) {
            throw new IllegalStateException("Could not load JDBC driver class [" + driverClassNameToUse + "]", var4);
        }

        if (logger.isInfoEnabled()) {
            logger.info("Loaded JDBC driver: " + driverClassNameToUse);
        }

    }

    protected Connection getConnectionFromDriver(Properties props) throws SQLException {
        String url = this.getUrl();
        if (logger.isDebugEnabled()) {
            logger.debug("Creating new JDBC DriverManager Connection to [" + url + "]");
        }

        return this.getConnectionFromDriverManager(url, props);
    }

    protected Connection getConnectionFromDriverManager(String url, Properties props) throws SQLException {
        return DriverManager.getConnection(url, props);
    }

    private static ClassLoader getDefaultClassLoader() {
        ClassLoader cl = null;

        try {
            cl = Thread.currentThread().getContextClassLoader();
        } catch (Throwable var2) {
            //ignore
        }

        if (cl == null) {
            cl = DriverManagerDataSource.class.getClassLoader();
        }

        return cl;
    }
}
