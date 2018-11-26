package com.jdbcagent.client;

import com.jdbcagent.client.jdbc.JdbcConnection;
import com.jdbcagent.client.netty.JdbcAgentNettyClient;
import com.jdbcagent.client.util.Util;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * JDBC-Agent client 基于netty的Datasource, 无须配置连接池
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class JdbcAgentDataSource implements DataSource {
    private volatile JdbcAgentNettyClient jdbcAgentNettyClient = null;  // netty客户端

    private String url;
    private String username;
    private String password;
    private String catalog;
    private String schema;
    private Properties connectionProperties;
    private int timeout = 30*60*1000;

    /**
     * 初始化方法
     *
     * @throws SQLException
     */
    public void init() throws SQLException {
        if (jdbcAgentNettyClient == null) {
            synchronized (JdbcAgentNettyClient.class) {
                if (jdbcAgentNettyClient == null) {
                    try {
                        Map<String, String> urlInfo = Util.parseUrl(url);

                        jdbcAgentNettyClient = new JdbcAgentNettyClient(this);
                        jdbcAgentNettyClient.setIp(urlInfo.get("ip"));
                        jdbcAgentNettyClient.setPort(Integer.parseInt(urlInfo.get("port")));
                        this.catalog = urlInfo.get("catalog");
                        jdbcAgentNettyClient.start();
                    } catch (Exception e) {
                        throw new SQLException(e);
                    }
                }
            }
        }
    }

    /**
     * 关闭netty client连接
     */
    public void close() {
        if (jdbcAgentNettyClient != null) {
            try {
                jdbcAgentNettyClient.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
            jdbcAgentNettyClient = null;
        }
    }

    public JdbcAgentDataSource() {
    }

    public JdbcAgentDataSource(String url) {
        this.setUrl(url);
    }

    public JdbcAgentDataSource(String url, String username, String password) {
        this.setUrl(url);
        this.setUsername(username);
        this.setPassword(password);
    }

    public JdbcAgentDataSource(String url, Properties conProps) {
        this.setUrl(url);
        this.setConnectionProperties(conProps);
    }

    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    public void setLoginTimeout(int timeout) throws SQLException {
        throw new UnsupportedOperationException("setLoginTimeout");
    }

    public PrintWriter getLogWriter() {
        throw new UnsupportedOperationException("getLogWriter");
    }

    public void setLogWriter(PrintWriter pw) throws SQLException {
        throw new UnsupportedOperationException("setLogWriter");
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return (T) this;
        } else {
            throw new SQLException("DataSource of type [" + this.getClass().getName() + "] cannot be unwrapped as [" + iface.getName() + "]");
        }
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    public Logger getParentLogger() {
        return Logger.getLogger("global");
    }


    public void setUrl(String url) {
        this.url = url.trim();
    }

    public String getUrl() {
        return this.url;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getUsername() {
        return this.username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getPassword() {
        return this.password;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public String getCatalog() {
        return this.catalog;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getSchema() {
        return this.schema;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public void setConnectionProperties(Properties connectionProperties) {
        this.connectionProperties = connectionProperties;
    }

    public Properties getConnectionProperties() {
        return this.connectionProperties;
    }

    public Connection getConnection() throws SQLException {
        return this.getConnectionFromDriver(this.getUsername(), this.getPassword());
    }

    public Connection getConnection(String username, String password) throws SQLException {
        return this.getConnectionFromDriver(username, password);
    }

    private Connection getConnectionFromDriver(String username, String password) throws SQLException {
        init();
        return new JdbcConnection(jdbcAgentNettyClient, catalog, username, password);
    }

    public void setDriverClassName(String driverClassName) {
        String driverClassNameToUse = driverClassName.trim();

        try {
            Class.forName(driverClassNameToUse, true, JdbcAgentDataSource.class.getClassLoader());
        } catch (ClassNotFoundException var4) {
            throw new IllegalStateException("Could not load JDBC driver class [" + driverClassNameToUse + "]", var4);
        }
    }
}
