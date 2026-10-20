package com.jdbcagent.server.config;

import com.jdbcagent.server.datasources.DataSourceFactory;
import org.apache.commons.lang.StringUtils;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

/**
 * JDBC-Agent server 端配置
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class JdbcAgentConf {
    private static Set<String> USER_MAP = new HashSet<>();                      // 用户名集合

    private static Map<DataSourceKey, DataSource> DATASOURCE_MAP =
            new HashMap<DataSourceKey, DataSource>();                          // 数据源集合，按目录和用户名查找

    private static final Map<String, String> ACCESS_PASSWORDS = new HashMap<String, String>();

    private static final byte[] DUMMY_PASSWORD = new byte[32];

    private JdbcAgent jdbcAgent;    // 配置对象

    /**
     * 初始化配置及数据源连接池
     */
    public void init() {
        if (jdbcAgent != null && jdbcAgent.dataSources != null) {
            ACCESS_PASSWORDS.clear();
            String catalog = StringUtils.trimToEmpty(jdbcAgent.getCatalog());
            for (DataSourceConf dsConf : jdbcAgent.dataSources) {
                String username = StringUtils.trimToEmpty(dsConf.getAccessUsername());
                String password = StringUtils.trimToEmpty(dsConf.getAccessPassword());
                if (username.isEmpty() || password.isEmpty()) {
                    throw new IllegalArgumentException("Error: empty access username or password");
                }
                if (USER_MAP.contains(username)) {
                    throw new RuntimeException("Error: duplicated access username");
                }

                USER_MAP.add(username);
                ACCESS_PASSWORDS.put(username, password);

                // 初始化数据源。密码只参与校验，不进入查找键
                DataSource dataSource = DataSourceFactory.getDataSource(dsConf);
                if (dataSource != null) {
                    DATASOURCE_MAP.put(new DataSourceKey(catalog, username), dataSource);
                }
            }
            USER_MAP.clear();
        }
    }

    public static DataSource getDataSource(String catalog, String username, String password) {
        if (!authenticate(username, password)) {
            return null;
        }
        return DATASOURCE_MAP.get(new DataSourceKey(catalog, username));
    }

    static Set<DataSourceKey> dataSourceKeys() {
        return Collections.unmodifiableSet(new HashSet<DataSourceKey>(DATASOURCE_MAP.keySet()));
    }

    /**
     * 数据源查找键。只包含目录和访问用户名，避免用竖线拼接后对错数据源，也不把密码留在键里。
     */
    public static final class DataSourceKey {
        private final String catalog;
        private final String username;

        public DataSourceKey(String catalog, String username) {
            this.catalog = StringUtils.trimToEmpty(catalog);
            this.username = StringUtils.trimToEmpty(username);
        }

        public String getCatalog() {
            return catalog;
        }

        public String getUsername() {
            return username;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof DataSourceKey)) {
                return false;
            }
            DataSourceKey key = (DataSourceKey) other;
            return catalog.equals(key.catalog) && username.equals(key.username);
        }

        @Override
        public int hashCode() {
            return 31 * catalog.hashCode() + username.hashCode();
        }
    }

    /**
     * 校验客户端握手时提交的访问账号。
     */
    public static boolean authenticate(String username, String password) {
        if (username == null || password == null) {
            return false;
        }
        String expected = ACCESS_PASSWORDS.get(StringUtils.trimToEmpty(username));
        byte[] actual = StringUtils.trimToEmpty(password).getBytes(StandardCharsets.UTF_8);
        byte[] expectedBytes = expected == null ? DUMMY_PASSWORD : expected.getBytes(StandardCharsets.UTF_8);
        return expected != null && MessageDigest.isEqual(actual, expectedBytes);
    }

    public JdbcAgent getJdbcAgent() {
        return jdbcAgent;
    }

    public void setJdbcAgent(JdbcAgent jdbcAgent) {
        this.jdbcAgent = jdbcAgent;
    }

    public static class JdbcAgent {
        private String zkServers;
        private String ip;
        private int port;
        private String catalog;
        private String sslKeyStore;
        private String sslKeyStorePassword;
        private String sslKeyStoreType;
        private List<DataSourceConf> dataSources;

        public String getZkServers() {
            return zkServers;
        }

        public void setZkServers(String zkServers) {
            this.zkServers = zkServers;
        }

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getCatalog() {
            return catalog;
        }

        public void setCatalog(String catalog) {
            this.catalog = catalog;
        }

        public String getSslKeyStore() {
            return sslKeyStore;
        }

        public void setSslKeyStore(String sslKeyStore) {
            this.sslKeyStore = sslKeyStore;
        }

        public String getSslKeyStorePassword() {
            return sslKeyStorePassword;
        }

        public void setSslKeyStorePassword(String sslKeyStorePassword) {
            this.sslKeyStorePassword = sslKeyStorePassword;
        }

        public String getSslKeyStoreType() {
            return sslKeyStoreType;
        }

        public void setSslKeyStoreType(String sslKeyStoreType) {
            this.sslKeyStoreType = sslKeyStoreType;
        }

        public List<DataSourceConf> getDataSources() {
            return dataSources;
        }

        public void setDataSources(List<DataSourceConf> dataSources) {
            this.dataSources = dataSources;
        }
    }
}
