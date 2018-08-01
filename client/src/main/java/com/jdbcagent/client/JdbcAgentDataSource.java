package com.jdbcagent.client;

import com.jdbcagent.client.jdbc.JdbcConnection;
import com.jdbcagent.client.netty.JdbcAgentNettyClient;
import com.jdbcagent.client.uitl.Util;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;
import java.util.logging.Logger;

/**
 * JDBC-Agent client 基于netty的Datasource, 无须配置连接池
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class JdbcAgentDataSource implements DataSource {
    private volatile JdbcAgentNettyClient jdbcAgentNettyClient = null;  // netty客户端

    private String url;                                                 // url地址

    private String catalog;                                             // 目录名, 从url解析

    private String username;                                            // 用户名

    private String password;                                            // 密码

    private int idleTimeout = 30 * 60 * 1000;                           // 连接超时时间

    public void setUrl(String url) {
        this.url = url;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setTimeout(int idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public String getUsername() {
        return username;
    }

    public int getTimeout() {
        return idleTimeout;
    }

    /**
     * 初始化方法
     *
     * @throws SQLException
     */
    public void init() throws SQLException {
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


    @Override
    public Connection getConnection() throws SQLException {
        init();
        return new JdbcConnection(jdbcAgentNettyClient, catalog, username, password);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        init();
        return new JdbcConnection(jdbcAgentNettyClient, catalog, username, password);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {

    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {

    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }
}
