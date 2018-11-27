package com.jdbcagent.client;

import com.jdbcagent.client.jdbc.JdbcConnection;
import com.jdbcagent.client.netty.DisconnectListener;
import com.jdbcagent.client.netty.JdbcAgentNettyClient;
import com.jdbcagent.client.util.Util;
import com.jdbcagent.client.util.ZookeeperUtil;
import com.jdbcagent.client.util.ZookeeperUtil.DataChangeListener;
import com.jdbcagent.client.util.loadbalance.RoundRobinLoadBalance;
import com.jdbcagent.core.util.ServerRunningData;
import com.jdbcagent.core.util.ZookeeperPathUtils;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * JDBC-Agent client 基于netty的Datasource, 无须配置连接池
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class JdbcAgentDataSource implements DataSource {
    public final ConcurrentHashMap<String, JdbcAgentNettyClient> jdbcAgentNettyClients = new ConcurrentHashMap<>();
    public final ConcurrentHashMap<String, ServerRunningData> serverRunningDatas = new ConcurrentHashMap<>();
    private volatile boolean initialed = false;

    private volatile JdbcAgentNettyClient jdbcAgentNettyClient = null;  // netty客户端


    private String url;
    private String username;
    private String password;
    private String catalog;
    private String schema;
    private Properties connectionProperties;
    private int timeout = 30 * 60 * 1000;

    private ZookeeperUtil zookeeperUtil;

    private DataChangeListener dataChangeListener;

    /**
     * 初始化方法
     *
     * @throws SQLException
     */
    public void init() throws SQLException {
        if (url.startsWith("jdbc:zookeeper")) {
            if (!initialed) {
                // 如果是HA模式
                Map<String, String> info = Util.parseZkUrl(url);             // 获取所有server地址
                if (info != null) {
                    zookeeperUtil = new ZookeeperUtil(info.get("zkServers"));

                    this.catalog = info.get("catalog");
                    List<ServerRunningData> serverRunningDataList =
                            ZookeeperUtil.getAllServers(info.get("zkServers"), info.get("catalog"));
                    for (ServerRunningData serverRunningData : serverRunningDataList) {
                        try {
                            registerClient(serverRunningData);
                        } catch (Exception e) {
                            throw new SQLException(e);
                        }
                    }

                    // 监听目录变化
                    zookeeperUtil.listenChilds(ZookeeperPathUtils.JA_ROOT_NODE +
                            ZookeeperPathUtils.SERVER_NODE +
                            ZookeeperPathUtils.ZOOKEEPER_SEPARATOR +
                            catalog, jdbcAgentNettyClients.keySet(), getDataChangeListener());
                }
                initialed = true;
            }
        } else {
            // 如果是单机模式
            if (jdbcAgentNettyClient == null) {
                synchronized (JdbcAgentNettyClient.class) {
                    if (jdbcAgentNettyClient == null) {
                        try {
                            Map<String, String> urlInfo = Util.parseUrl(url);

                            jdbcAgentNettyClient = new JdbcAgentNettyClient(timeout);
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
    }

    private DataChangeListener getDataChangeListener() {
        if (dataChangeListener == null) {
            synchronized (JdbcAgentDataSource.class) {
                if (dataChangeListener == null) {
                    dataChangeListener = new DataChangeListener() {
                        @Override
                        public void onAdd(final ServerRunningData serverRunningData) {
                            try {
                                Thread.sleep(5000);
                            } catch (InterruptedException e) {
                                // ignore
                            }
                            registerClient(serverRunningData);
                        }

                        @Override
                        public void onRemove(String key) {
                            JdbcAgentNettyClient jdbcAgentNettyClient = jdbcAgentNettyClients.get(key);
                            if (jdbcAgentNettyClient != null) {
                                jdbcAgentNettyClient.stop();
                            }
                        }
                    };
                }
            }
        }
        return dataChangeListener;
    }

    private void registerClient(final ServerRunningData serverRunningData) {
        JdbcAgentNettyClient jdbcAgentNettyClientTmp =
                new JdbcAgentNettyClient(timeout,
                        new DisconnectListener() {
                            @Override
                            public void onDisconnect() {
                                jdbcAgentNettyClients.remove(serverRunningData.getAddress());
                                serverRunningDatas.remove(serverRunningData.getAddress());
                            }
                        });
        String[] ipPort = serverRunningData.getAddress().split(":");
        jdbcAgentNettyClientTmp.setIp(ipPort[0]);
        jdbcAgentNettyClientTmp.setPort(Integer.parseInt(ipPort[1]));
        jdbcAgentNettyClientTmp.start();
        jdbcAgentNettyClients.putIfAbsent(serverRunningData.getAddress(), jdbcAgentNettyClientTmp);
        serverRunningDatas.putIfAbsent(serverRunningData.getAddress(), serverRunningData);

        zookeeperUtil.listenData(ZookeeperPathUtils.JA_ROOT_NODE +
                ZookeeperPathUtils.SERVER_NODE +
                ZookeeperPathUtils.ZOOKEEPER_SEPARATOR +
                catalog + ZookeeperPathUtils.ZOOKEEPER_SEPARATOR +
                serverRunningData.getAddress(), jdbcAgentNettyClients.keySet(), getDataChangeListener());
    }

    private JdbcAgentNettyClient getJdbcAgentNettyClient() {
        if (jdbcAgentNettyClient != null) {
            return jdbcAgentNettyClient;
        } else {
            List<ServerRunningData> list = new ArrayList<>(serverRunningDatas.size());
            list.addAll(serverRunningDatas.values());
            if (list.isEmpty()) {
                throw new RuntimeException("Empty jdbc agent server for load");
            }
            ServerRunningData selectedServer = RoundRobinLoadBalance.doSelect(list);
            JdbcAgentNettyClient jdbcAgentNettyClient = jdbcAgentNettyClients.get(selectedServer.getAddress());
            if (jdbcAgentNettyClient.getConnected().get()) {
                return jdbcAgentNettyClient;
            } else {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    //ignore
                }
                return getJdbcAgentNettyClient();
            }
        }
    }

    /**
     * 关闭netty client连接
     */
    public void close() {
        if (zookeeperUtil != null) {
            zookeeperUtil.close();
        }
        if (jdbcAgentNettyClient != null) {
            try {
                jdbcAgentNettyClient.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
            jdbcAgentNettyClient = null;
        } else {
            for (JdbcAgentNettyClient jdbcAgentNettyClientTmp : jdbcAgentNettyClients.values()) {
                jdbcAgentNettyClientTmp.stop();
            }
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

    public java.util.logging.Logger getParentLogger() {
        return java.util.logging.Logger.getLogger("global");
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
        return new JdbcConnection(getJdbcAgentNettyClient(), catalog, username, password);
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
