package com.jdbcagent.server.config;

import com.jdbcagent.server.datasources.DataSourceFactory;
import org.apache.commons.lang.StringUtils;

import javax.sql.DataSource;
import java.util.*;

/**
 * JDBC-Agent server 端配置
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class JdbcAgentConf {
    private static Set<String> USER_MAP = new HashSet<>();                      // 用户名集合

    private static Map<String, DataSource> DATASOURCE_MAP = new HashMap<>();    // 数据源集合

    private JdbcAgent jdbcAgent;    // 配置对象

    /**
     * 初始化配置及数据源连接池
     */
    public void init() {
        if (jdbcAgent != null && jdbcAgent.dataSources != null) {
            for (DataSourceConf dsConf : jdbcAgent.dataSources) {
                if (StringUtils.isEmpty(dsConf.getAccessUsername())
                        || StringUtils.isEmpty(dsConf.getAccessPassword())) {
                    throw new IllegalArgumentException("Error: empty access username or password");
                }
                if (USER_MAP.contains(dsConf.getAccessUsername())) {
                    throw new RuntimeException("Error: duplicated access username");
                }

                USER_MAP.add(dsConf.getAccessUsername());

                // 初始化数据源
                DataSource dataSource = DataSourceFactory.getDataSource(dsConf);
                if (dataSource != null) {
                    String key = StringUtils.trimToEmpty(jdbcAgent.getCatalog()) + "|"
                            + StringUtils.trimToEmpty(dsConf.getAccessUsername()) + "|"
                            + StringUtils.trimToEmpty(dsConf.getAccessPassword());
                    DATASOURCE_MAP.put(key, dataSource);
                }
            }
            USER_MAP.clear();
        }
    }

    public static DataSource getDataSource(String key) {
        return DATASOURCE_MAP.get(key);
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

        public List<DataSourceConf> getDataSources() {
            return dataSources;
        }

        public void setDataSources(List<DataSourceConf> dataSources) {
            this.dataSources = dataSources;
        }
    }
}
