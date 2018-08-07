package com.jdbcagent.server.datasources;

import com.alibaba.druid.pool.DruidDataSource;
import com.jdbcagent.server.config.DataSourceConf;
import com.jdbcagent.server.config.DruidDataSourceConf;
import com.jdbcagent.server.config.HiKariCPConf;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * JDBC-Agent server 数据源工程类
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class DataSourceFactory {
    private static Logger logger = LoggerFactory.getLogger(DataSourceFactory.class);

    public static ConcurrentHashMap<String, DataSource> DATA_SOURCES_MAP = new ConcurrentHashMap<>();    // 数据源集合

    /**
     * 通过连接池类型创建获取一个数据源对象
     *
     * @param dsConf 数据源配置
     * @return 数据源
     */
    public static DataSource getDataSource(DataSourceConf dsConf) {
        if (dsConf.getDruid() != null) {
            DruidDataSourceConf druidConf = dsConf.getDruid();
            DruidDataSource druidDataSource = new DruidDataSource();
            druidDataSource.setDriverClassName(druidConf.getDriverClass());
            druidDataSource.setUrl(druidConf.getJdbcUrl());
            druidDataSource.setUsername(druidConf.getUsername());
            druidDataSource.setPassword(druidConf.getPassword());
            if (druidConf.getConnectProperties() != null) {
                druidDataSource.setConnectProperties(druidConf.getConnectProperties());
            }
            if (druidConf.getInitialSize() != null) {
                druidDataSource.setInitialSize(druidConf.getInitialSize());
            }
            if (druidConf.getMinIdle() != null) {
                druidDataSource.setMinIdle(druidConf.getMinIdle());
            }
            if (druidConf.getMaxActive() != null) {
                druidDataSource.setMaxActive(druidConf.getMaxActive());
            }
            if (druidConf.getMaxWait() != null) {
                druidDataSource.setMaxWait(druidConf.getMaxWait());
            }
            if (druidConf.getTimeBetweenEvictionRunsMillis() != null) {
                druidDataSource.setTimeBetweenEvictionRunsMillis(druidConf.getTimeBetweenEvictionRunsMillis());
            }
            if (druidConf.getMinEvictableIdleTimeMillis() != null) {
                druidDataSource.setMinEvictableIdleTimeMillis(druidConf.getMinEvictableIdleTimeMillis());
            }
            if (druidConf.getNotFullTimeoutRetryCount() != null) {
                druidDataSource.setNotFullTimeoutRetryCount(druidConf.getNotFullTimeoutRetryCount());
            }
            if (druidConf.getPoolPreparedStatements() != null) {
                druidDataSource.setPoolPreparedStatements(druidConf.getPoolPreparedStatements());
            }
            if (druidConf.getSharePreparedStatements() != null) {
                druidDataSource.setSharePreparedStatements(druidConf.getSharePreparedStatements());
            }
            if (druidConf.getMaxPoolPreparedStatementPerConnectionSize() != null) {
                druidDataSource.setMaxPoolPreparedStatementPerConnectionSize(druidConf
                        .getMaxPoolPreparedStatementPerConnectionSize());
            }
            if (druidConf.getValidationQuery() != null) {
                druidDataSource.setValidationQuery(druidConf.getValidationQuery());
            }
            if (druidConf.getValidationQueryTimeout() != null) {
                druidDataSource.setValidationQueryTimeout(druidConf.getValidationQueryTimeout());
            }
            if (druidConf.getTestWhileIdle() != null) {
                druidDataSource.setTestWhileIdle(druidConf.getTestWhileIdle());
            }
            if (druidConf.getTestOnBorrow() != null) {
                druidDataSource.setTestOnBorrow(druidConf.getTestOnBorrow());
            }
            if (druidConf.getTestOnReturn() != null) {
                druidDataSource.setTestOnReturn(druidConf.getTestOnReturn());
            }
            try {
                druidDataSource.init();
            } catch (SQLException e) {
                logger.error("Error: init data source {}-{}", druidConf.getJdbcUrl(), druidConf.getUsername());
            }
            return druidDataSource;
        } else if (dsConf.getHikari() != null) {
            HiKariCPConf hiKariCPConf = dsConf.getHikari();
            HikariDataSource hikariDataSource = new HikariDataSource();
            hikariDataSource.setDriverClassName(hiKariCPConf.getDriverClassName());
            hikariDataSource.setJdbcUrl(hiKariCPConf.getJdbcUrl());
            hikariDataSource.setUsername(hiKariCPConf.getUsername());
            hikariDataSource.setPassword(hiKariCPConf.getPassword());
            hikariDataSource.setMaximumPoolSize(hiKariCPConf.getMaxPoolSize());
            hikariDataSource.setMinimumIdle(hiKariCPConf.getMinIdle());
            if (hiKariCPConf.getIdleTimeout() != null) {
                hikariDataSource.setIdleTimeout(hiKariCPConf.getIdleTimeout());
            }
            if (hiKariCPConf.getMaxLifetime() != null) {
                hikariDataSource.setMaxLifetime(hiKariCPConf.getMaxLifetime());
            }
            if (hiKariCPConf.getCatalog() != null) {
                hikariDataSource.setCatalog(hiKariCPConf.getCatalog());
            }
            if (hiKariCPConf.getConnectionTimeout() != null) {
                hikariDataSource.setConnectionTimeout(hiKariCPConf.getConnectionTimeout());
            }
            if (hiKariCPConf.getValidationTimeout() != null) {
                hikariDataSource.setValidationTimeout(hiKariCPConf.getValidationTimeout());
            }
            if (hiKariCPConf.getInitializationFailTimeout() != null) {
                hikariDataSource.setInitializationFailTimeout(hiKariCPConf.getInitializationFailTimeout());
            }
            if (hiKariCPConf.getConnectionInitSql() != null) {
                hikariDataSource.setConnectionInitSql(hiKariCPConf.getConnectionInitSql());
            }
            if (hiKariCPConf.getConnectionTestQuery() != null) {
                hikariDataSource.setConnectionTestQuery(hiKariCPConf.getConnectionTestQuery());
            }
            if (hiKariCPConf.getDataSourceJndiName() != null) {
                hikariDataSource.setDataSourceJNDI(hiKariCPConf.getDataSourceJndiName());
            }
            if (hiKariCPConf.getPoolName() != null) {
                hikariDataSource.setPoolName(hiKariCPConf.getPoolName());
            }
            if (hiKariCPConf.getSchema() != null) {
                hikariDataSource.setSchema(hiKariCPConf.getSchema());
            }
            if (hiKariCPConf.getTransactionIsolationName() != null) {
                hikariDataSource.setTransactionIsolation(hiKariCPConf.getTransactionIsolationName());
            }
            if (hiKariCPConf.getAutoCommit() != null) {
                hikariDataSource.setAutoCommit(hiKariCPConf.getAutoCommit());
            }
            if (hiKariCPConf.getReadOnly() != null) {
                hikariDataSource.setReadOnly(hiKariCPConf.getReadOnly());
            }
            if (hiKariCPConf.getIsolateInternalQueries() != null) {
                hikariDataSource.setIsolateInternalQueries(hiKariCPConf.getIsolateInternalQueries());
            }
            if (hiKariCPConf.getRegisterMbeans() != null) {
                hikariDataSource.setRegisterMbeans(hiKariCPConf.getRegisterMbeans());
            }
            if (hiKariCPConf.getAlowPoolSuspension() != null) {
                hikariDataSource.setAllowPoolSuspension(hiKariCPConf.getAlowPoolSuspension());
            }
            return hikariDataSource;
        }
        return null;
    }
}
