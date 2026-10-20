package com.jdbcagent.client;

import com.jdbcagent.client.jdbc.JdbcConnection;
import com.jdbcagent.client.netty.JdbcAgentNettyClient;
import com.jdbcagent.client.uitl.Util;
import com.jdbcagent.client.uitl.ZookeeperUtil;
import com.jdbcagent.core.util.TransportSecurity;
import io.netty.channel.Channel;

import javax.net.ssl.SSLContext;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
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

    private String sslTrustStore;                                       // 信任库

    private String sslTrustStorePassword;

    private String sslTrustStoreType;

    private String zkServers;                                           // zookeeper 地址

    private volatile String activeAddress;                              // 当前 active host:port

    private volatile boolean closed;                                    // 调用方已关闭

    private boolean watching;                                           // 是否已监听主备切换

    private final ReentrantLock connectLock = new ReentrantLock();

    private final AtomicBoolean reconnecting = new AtomicBoolean();

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

    public void setSslTrustStore(String sslTrustStore) {
        this.sslTrustStore = sslTrustStore;
    }

    public void setSslTrustStorePassword(String sslTrustStorePassword) {
        this.sslTrustStorePassword = sslTrustStorePassword;
    }

    public void setSslTrustStoreType(String sslTrustStoreType) {
        this.sslTrustStoreType = sslTrustStoreType;
    }

    public void setTimeout(int idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
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
        connectLock.lock();
        try {
            closed = false;
            if (jdbcAgentNettyClient != null) {
                return;
            }
            Map<String, String> urlInfo = Util.parseUrl(url);
            this.catalog = urlInfo.get("catalog");
            this.zkServers = urlInfo.get("zkServers");
            if (zkServers != null && !watching) {
                watching = true;
                ZookeeperUtil.watch(zkServers, catalog, new ZookeeperUtil.AddressListener() {
                    @Override
                    public void onAddress(String address) {
                        switchTo(address);
                    }
                });
            }
            String ipPort = activeAddress != null
                    ? activeAddress
                    : urlInfo.get("ip") + ":" + urlInfo.get("port");
            activeAddress = ipPort;
            SSLContext sslContext = TransportSecurity.clientContext(
                    sslTrustStore, sslTrustStorePassword, sslTrustStoreType);
            JdbcAgentNettyClient client = new JdbcAgentNettyClient(this);
            client.setSslContext(sslContext);
            applyAddress(client, ipPort);
            jdbcAgentNettyClient = client;
            try {
                client.start();
            } catch (RuntimeException e) {
                client.stop();
                jdbcAgentNettyClient = null;
                throw e;
            }
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException(e);
        } finally {
            connectLock.unlock();
        }
    }

    /**
     * 当前通道仍是正在使用的连接时返回 true。重连过程中旧通道关闭会返回 false。
     */
    public boolean isCurrentChannel(Channel channel) {
        JdbcAgentNettyClient client = jdbcAgentNettyClient;
        return client != null && channel != null && channel == client.getChannel();
    }

    /**
     * zookeeper 模式下通道断开后等待新的 active 并重连。
     *
     * @return true 表示已接管重连，调用方不要再关闭 DataSource
     */
    public boolean reconnectAfterClose() {
        if (zkServers == null || closed) {
            return false;
        }
        if (!reconnecting.compareAndSet(false, true)) {
            return true;
        }
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    long deadline = System.nanoTime() + ZookeeperUtil.FAILOVER_WAIT_MILLIS * 1000L * 1000L;
                    while (!closed && System.nanoTime() < deadline) {
                        try {
                            switchTo(ZookeeperUtil.getAddressFromZk(zkServers, catalog));
                            return;
                        } catch (Exception ignored) {
                            try {
                                Thread.sleep(200);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                        }
                    }
                } finally {
                    reconnecting.set(false);
                }
            }
        }, "jdbc-agent-failover");
        thread.setDaemon(true);
        thread.start();
        return true;
    }

    private void switchTo(String ipPort) {
        if (ipPort == null || closed) {
            return;
        }
        connectLock.lock();
        try {
            if (closed) {
                return;
            }
            JdbcAgentNettyClient client = jdbcAgentNettyClient;
            boolean same = ipPort.equals(activeAddress);
            activeAddress = ipPort;
            if (client == null) {
                return;
            }
            Channel channel = client.getChannel();
            if (same && channel != null && channel.isActive()) {
                return;
            }
            applyAddress(client, ipPort);
            client.reconnect();
        } finally {
            connectLock.unlock();
        }
    }

    private static void applyAddress(JdbcAgentNettyClient client, String ipPort) {
        int colon = ipPort.lastIndexOf(':');
        client.setIp(ipPort.substring(0, colon));
        client.setPort(Integer.parseInt(ipPort.substring(colon + 1)));
    }

    /**
     * 关闭netty client连接
     */
    public void close() {
        connectLock.lock();
        try {
            closed = true;
            if (jdbcAgentNettyClient != null) {
                try {
                    jdbcAgentNettyClient.stop();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                jdbcAgentNettyClient = null;
            }
        } finally {
            connectLock.unlock();
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
