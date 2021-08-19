package com.jdbcagent.server.datasources;

import com.alibaba.druid.pool.DruidDataSource;
import com.jdbcagent.core.support.datasource.DriverManagerDataSource;
import com.jdbcagent.server.config.DataSourceConf;
import com.jdbcagent.server.config.DruidDataSourceConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * JDBC-Agent server 数据源工程类
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class DataSourceFactory {
    private static Logger logger = LoggerFactory.getLogger(DataSourceFactory.class);

    /**
     * 通过连接池类型创建获取一个数据源对象
     *
     * @param dsConf 数据源配置
     * @return 数据源
     */
    public static DataSource getDataSource(DataSourceConf dsConf) {
        if (dsConf.getDsManager() != null) {
            Map<String, Object> dsManagerConf = dsConf.getDsManager();
            Map<String, Object> dsManagerConfTmp = new LinkedHashMap<>(dsManagerConf);
            DriverManagerDataSource dmDataSource = new DriverManagerDataSource();
            dmDataSource.setUrl((String) dsManagerConfTmp.remove("jdbcUrl"));
            dmDataSource.setUsername((String) dsManagerConfTmp.remove("username"));
            dmDataSource.setPassword((String) dsManagerConfTmp.remove("password"));
            dmDataSource.setDriverClassName((String) dsManagerConfTmp.remove("driverClass"));
            if (!dsManagerConfTmp.isEmpty()) {
                Properties conProps = new Properties();
                for (Map.Entry<String, Object> entry : dsManagerConfTmp.entrySet()) {
                    conProps.put(entry.getKey(), entry.getValue());
                }
                dmDataSource.setConnectionProperties(conProps);
            }
            return dmDataSource;
        } else if (dsConf.getDruid() != null) {
            DruidDataSourceConf druidConf = dsConf.getDruid();
            DruidDataSource druidDataSource = new DruidDataSource();
            druidDataSource.setDriverClassName(druidConf.getDriverClass());
            druidDataSource.setUrl(druidConf.getJdbcUrl());
            druidDataSource.setUsername(druidConf.getUsername());
            druidDataSource.setPassword(druidConf.getPassword());
            druidDataSource.setConnectProperties(druidConf.getConnectProperties());
            druidDataSource.setInitialSize(druidConf.getInitialSize());
            druidDataSource.setMinIdle(druidConf.getMinIdle());
            druidDataSource.setMaxActive(druidConf.getMaxActive());
            druidDataSource.setMaxWait(druidConf.getMaxWait());
            druidDataSource.setTimeBetweenEvictionRunsMillis(druidConf.getTimeBetweenEvictionRunsMillis());
            druidDataSource.setMinEvictableIdleTimeMillis(druidConf.getMinEvictableIdleTimeMillis());
            druidDataSource.setNotFullTimeoutRetryCount(druidConf.getNotFullTimeoutRetryCount());
            druidDataSource.setPoolPreparedStatements(druidConf.isPoolPreparedStatements());
            druidDataSource.setSharePreparedStatements(druidConf.isSharePreparedStatements());
            druidDataSource.setMaxPoolPreparedStatementPerConnectionSize(druidConf.getMaxPoolPreparedStatementPerConnectionSize());
            druidDataSource.setValidationQuery(druidConf.getValidationQuery());
            druidDataSource.setValidationQueryTimeout(druidConf.getValidationQueryTimeout());
            druidDataSource.setTestWhileIdle(druidConf.isTestWhileIdle());
            druidDataSource.setTestOnBorrow(druidConf.isTestOnBorrow());
            druidDataSource.setTestOnReturn(druidConf.isTestOnReturn());


            try {
                druidDataSource.init();
            } catch (SQLException e) {
                logger.error("Error: init data source {}-{}", druidConf.getJdbcUrl(), druidConf.getUsername());
            }
            return druidDataSource;
        }
        return null;
    }
}
