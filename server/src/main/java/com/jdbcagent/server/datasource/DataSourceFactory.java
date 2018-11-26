package com.jdbcagent.server.datasource;

import com.jdbcagent.server.config.DataSourceConf;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * JDBC-Agent server 数据源工程类
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class DataSourceFactory {
    public static ConcurrentHashMap<String, DataSource> DATA_SOURCES_MAP = new ConcurrentHashMap<>();          // 数据源集合
    public static ConcurrentHashMap<String, DataSource> WRITE_DATA_SOURCES_MAP = new ConcurrentHashMap<>();    // 写数据源集合
    public static ConcurrentHashMap<String, DataSource> READE_DATA_SOURCES_MAP = new ConcurrentHashMap<>();    // 读数据源集合

    /**
     * 通过连接池类型创建获取一个数据源对象
     *
     * @param dsConf 数据源配置
     * @return 数据源
     */
    public static DataSource createDataSource(String catalog, DataSourceConf dsConf) {
        if (dsConf.getDsClassName() != null) {
            try {
                Class cpClass = Class.forName(dsConf.getDsClassName());
                if (!DataSource.class.isAssignableFrom(cpClass)) {
                    throw new RuntimeException("dsClassName is not a DataSource class name");
                }
                DataSource dataSource;
                Map<String, String> dsProperties = dsConf.getDsProperties();
                if (dsProperties != null) {
                    dataSource = (DataSource) cpClass.newInstance();
                    for (Map.Entry<String, String> property : dsProperties.entrySet()) {
                        setProperty(dataSource, property.getKey(), property.getValue());
                    }
                } else {
                    Map<String, String> writeDsProperties = dsConf.getWriterDsProperties();
                    Map<String, String> readDsProperties = dsConf.getReaderDsProperties();
                    if (writeDsProperties == null || readDsProperties == null) {
                        throw new RuntimeException("Empty properties of data source");
                    }
                    DataSource writeDataSource = (DataSource) cpClass.newInstance();
                    for (Map.Entry<String, String> property : writeDsProperties.entrySet()) {
                        setProperty(writeDataSource, property.getKey(), property.getValue());
                    }
                    DataSourceFactory.WRITE_DATA_SOURCES_MAP.put(
                            catalog + "|" +
                                    dsConf.getAccessUsername() + "|" +
                                    dsConf.getAccessPassword(), writeDataSource);
                    DataSource readDataSource = (DataSource) cpClass.newInstance();
                    for (Map.Entry<String, String> property : readDsProperties.entrySet()) {
                        setProperty(readDataSource, property.getKey(), property.getValue());
                    }
                    DataSourceFactory.READE_DATA_SOURCES_MAP.put(
                            catalog + "|" +
                                    dsConf.getAccessUsername() + "|" +
                                    dsConf.getAccessPassword(), readDataSource);

                    String readDsPrimary = readDsProperties.get("primary");
                    if (readDsPrimary != null && readDsPrimary.equalsIgnoreCase("true")) {
                        dataSource = readDataSource;
                    } else {
                        // 默认以写数据源为主数据源
                        dataSource = writeDataSource;
                    }
                }
                if (dataSource != null) {
                    DataSourceFactory.DATA_SOURCES_MAP.put(
                            catalog + "|" +
                                    dsConf.getAccessUsername() + "|" +
                                    dsConf.getAccessPassword(), dataSource);
                }
                return dataSource;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return null;
    }


    private static void setProperty(Object obj, String fieldName, String value) throws InvocationTargetException, IllegalAccessException {
        if (value == null) {
            return;
        }

        Class<?> clazz = obj.getClass();
        Method[] methods = clazz.getMethods();
        for (Method method : methods) {
            if (method.getName().equals("set" + Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1))) {
                Class[] paramTypes = method.getParameterTypes();
                if (paramTypes.length > 0) {
                    if (String.class.isAssignableFrom(paramTypes[0])) {
                        method.invoke(obj, value);
                    } else if (Integer.class.isAssignableFrom(paramTypes[0]) || int.class.isAssignableFrom(paramTypes[0])) {
                        method.invoke(obj, Integer.parseInt(value));

                    } else if (Long.class.isAssignableFrom(paramTypes[0]) || long.class.isAssignableFrom(paramTypes[0])) {
                        method.invoke(obj, Long.parseLong(value));
                    }
                }
                break;
            }
        }
    }
}
