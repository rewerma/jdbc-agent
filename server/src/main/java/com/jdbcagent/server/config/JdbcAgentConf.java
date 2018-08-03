package com.jdbcagent.server.config;

import com.alibaba.druid.pool.DruidDataSource;
import com.jdbcagent.server.datasources.DataSourceFactory;
import org.apache.commons.lang.StringUtils;

import javax.sql.DataSource;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * JDBC-Agent server 端配置
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class JdbcAgentConf {
    private static Set<String> USER_MAP = new HashSet<>();  // 用户名集合

    private JdbcAgent jdbcAgent;                            // 配置对象

    /**
     * 初始化配置
     */
    public void init() {
        if (jdbcAgent != null && jdbcAgent.getCatalogs() != null) {
            // 验证是否有相同的catalog和username
            for (Catalog catalog : jdbcAgent.getCatalogs()) {
                for (DataSourceConf dsConf : catalog.getDataSources()) {
                    if (StringUtils.isEmpty(dsConf.getAccessUsername())
                            || StringUtils.isEmpty(dsConf.getAccessPassword())) {
                        throw new IllegalArgumentException("Error: empty access username or password of catalog: " + catalog.getCatalog());
                    }
                    if (USER_MAP.contains(catalog.getCatalog() + "|" + dsConf.getAccessUsername())) {
                        throw new RuntimeException("Error: duplicated catalog and username");
                    }

                    USER_MAP.add(catalog.getCatalog() + "|" + dsConf.getAccessUsername());
                }
            }
            USER_MAP.clear();
        }
    }

    /**
     * 初始化指定数据源
     *
     * @param catalog 目录配置项
     */
    public void initDataSource(Catalog catalog) {
        for (DataSourceConf dsConf : catalog.getDataSources()) {
            DataSource dataSource = DataSourceFactory.DATA_SOURCES_MAP.get(
                    catalog.getCatalog() + "|" +
                            dsConf.getAccessUsername() + "|" +
                            dsConf.getAccessPassword());
            boolean closed = false;
            if (dataSource == null) {
                closed = true;
            } else {
                if (dataSource instanceof DruidDataSource &&
                        ((DruidDataSource) dataSource).isClosed()) {
                    closed = true;
                }
            }
            if (closed) {
                dataSource = DataSourceFactory.getDataSource(dsConf);
                DataSourceFactory.DATA_SOURCES_MAP.put(
                        catalog.getCatalog() + "|" +
                                dsConf.getAccessUsername() + "|" +
                                dsConf.getAccessPassword(), dataSource);
            }
        }
    }

    /**
     * 初始化所有数据源
     */
    public void initAllDS() {
        for (Catalog catalog : jdbcAgent.getCatalogs()) {
            initDataSource(catalog);
        }
    }

    /**
     * 关闭指定数据源
     *
     * @param catalog 目录名配置项
     */
    public void closeDataSource(Catalog catalog) {
        for (DataSourceConf dsConf : catalog.getDataSources()) {
            DataSource dataSource = DataSourceFactory.DATA_SOURCES_MAP.get(
                    catalog.getCatalog() + "|" +
                            dsConf.getAccessUsername() + "|" +
                            dsConf.getAccessPassword());
            if (dataSource != null) {
                if (dataSource instanceof DruidDataSource) {
                    DruidDataSource druidDataSource = (DruidDataSource) dataSource;
                    druidDataSource.close();
                }
            }
        }
    }

    /**
     * 关闭所有数据源
     */
    public void closeAllDS() {
        for (Catalog catalog : jdbcAgent.getCatalogs()) {
            closeDataSource(catalog);
        }
    }

    /**
     * 通过键获取数据源
     *
     * @param key
     * @return
     */
    public static DataSource getDataSource(String key) {
        return DataSourceFactory.DATA_SOURCES_MAP.get(key);
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
        private List<Catalog> catalogs;

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

        public List<Catalog> getCatalogs() {
            return catalogs;
        }

        public void setCatalogs(List<Catalog> catalogs) {
            this.catalogs = catalogs;
        }
    }

    public static class Catalog {
        private String catalog;
        private List<DataSourceConf> dataSources;

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
