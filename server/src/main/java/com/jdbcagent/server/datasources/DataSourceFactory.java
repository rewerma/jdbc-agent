package com.jdbcagent.server.datasources;

import com.jdbcagent.core.support.datasource.DriverManagerDataSource;
import com.jdbcagent.server.config.DataSourceConf;
import com.jdbcagent.server.plugin.JdbcDriverPlugins;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
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
        DataSource dataSource = createDataSource(dsConf);
        if (dataSource == null) {
            return null;
        }
        Integer maxConnections = dsConf.getMaxConnections();
        if (maxConnections == null) {
            return dataSource;
        }
        if (maxConnections < 0) {
            throw new IllegalArgumentException("Error: maxConnections must be >= 0");
        }
        return new LimitedDataSource(dataSource, maxConnections);
    }

    private static DataSource createDataSource(DataSourceConf dsConf) {
        if (dsConf.getDsManager() != null) {
            Map<String, Object> dsManagerConf = dsConf.getDsManager();
            Map<String, Object> dsManagerConfTmp = new LinkedHashMap<>(dsManagerConf);
            DriverManagerDataSource dmDataSource = new DriverManagerDataSource();
            dmDataSource.setUrl((String) dsManagerConfTmp.remove("jdbcUrl"));
            dmDataSource.setUsername((String) dsManagerConfTmp.remove("username"));
            dmDataSource.setPassword((String) dsManagerConfTmp.remove("password"));
            String driverClass = (String) dsManagerConfTmp.remove("driverClass");
            ClassLoader driverLoader = null;
            if (driverClass != null) {
                // 驱动放在插件自己的类加载器里，server 的 lib 不再带 mysql-connector 这类包
                JdbcDriverPlugins.install(driverClass);
                driverLoader = JdbcDriverPlugins.classLoader(driverClass);
            }
            ClassLoader previous = Thread.currentThread().getContextClassLoader();
            try {
                if (driverLoader != null) {
                    Thread.currentThread().setContextClassLoader(driverLoader);
                }
                dmDataSource.setDriverClassName(driverClass);
            } finally {
                Thread.currentThread().setContextClassLoader(previous);
            }
            if (!dsManagerConfTmp.isEmpty()) {
                Properties conProps = new Properties();
                for (Map.Entry<String, Object> entry : dsManagerConfTmp.entrySet()) {
                    conProps.put(entry.getKey(), entry.getValue());
                }
                dmDataSource.setConnectionProperties(conProps);
            }
            return dmDataSource;
        }
        return null;
    }
}
