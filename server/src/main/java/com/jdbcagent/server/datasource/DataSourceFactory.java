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
    public static ConcurrentHashMap<String, DataSource> DATA_SOURCES_MAP = new ConcurrentHashMap<>();    // 数据源集合

    /**
     * 通过连接池类型创建获取一个数据源对象
     *
     * @param dsConf 数据源配置
     * @return 数据源
     */
    public static DataSource getDataSource(DataSourceConf dsConf) {
        if (dsConf.getDsClassName() != null) {
            try {
                Class cpClass = Class.forName(dsConf.getDsClassName());
                if (!DataSource.class.isAssignableFrom(cpClass)) {
                    throw new RuntimeException("dsClassName is not a DataSource class name");
                }
                Object dataSource = cpClass.newInstance();
                Map<String, String> dsProperties = dsConf.getDsProperties();
                for (Map.Entry<String, String> property : dsProperties.entrySet()) {
                    setProperty(dataSource, property.getKey(), property.getValue());
                }
                return (DataSource) dataSource;
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
