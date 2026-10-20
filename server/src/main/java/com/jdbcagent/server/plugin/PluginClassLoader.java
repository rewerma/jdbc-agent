package com.jdbcagent.server.plugin;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;

/**
 * 一个插件目录对应一个类加载器。插件自己的类优先，JDBC API、SPI 接口、日志和 server 本身仍用父加载器，
 * 这样各插件的驱动和依赖互不可见，又不会拆出两套 java.sql.Driver。
 */
public class PluginClassLoader extends URLClassLoader {

    PluginClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            Class<?> loaded = findLoadedClass(name);
            if (loaded != null) {
                return loaded;
            }
            if (parentFirst(name)) {
                return super.loadClass(name, resolve);
            }
            try {
                Class<?> type = findClass(name);
                if (resolve) {
                    resolveClass(type);
                }
                return type;
            } catch (ClassNotFoundException notInPlugin) {
                return super.loadClass(name, resolve);
            }
        }
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        // 只认插件 jar 里的 SPI，避免 server classpath 上的同名服务被算进这个插件
        if ("META-INF/services/com.jdbcagent.plugin.JdbcDriverPlugin".equals(name)) {
            return findResources(name);
        }
        return super.getResources(name);
    }

    @Override
    public URL getResource(String name) {
        if (name != null && parentFirstResource(name)) {
            return super.getResource(name);
        }
        URL local = findResource(name);
        if (local != null) {
            return local;
        }
        return super.getResource(name);
    }

    private static boolean parentFirst(String name) {
        return name.startsWith("java.")
                || name.startsWith("javax.")
                || name.startsWith("jdk.")
                || name.startsWith("sun.")
                || name.startsWith("com.sun.")
                || name.startsWith("org.w3c.")
                || name.startsWith("org.xml.")
                || name.startsWith("org.slf4j.")
                || name.startsWith("com.jdbcagent.core.")
                || name.startsWith("com.jdbcagent.server.")
                || name.startsWith("com.jdbcagent.client.")
                || name.equals("com.jdbcagent.plugin.JdbcDriverPlugin");
    }

    private static boolean parentFirstResource(String name) {
        return name.startsWith("java/")
                || name.startsWith("javax/")
                || name.startsWith("com/jdbcagent/core/")
                || name.startsWith("com/jdbcagent/server/")
                || name.startsWith("com/jdbcagent/client/")
                || name.startsWith("com/jdbcagent/plugin/JdbcDriverPlugin")
                || name.startsWith("org/slf4j/");
    }
}
