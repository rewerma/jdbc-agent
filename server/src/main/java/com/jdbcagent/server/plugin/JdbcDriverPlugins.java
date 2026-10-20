package com.jdbcagent.server.plugin;

import com.jdbcagent.plugin.JdbcDriverPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;

/**
 * 从插件目录按 SPI 装入 JDBC 驱动。每个子目录或顶层 jar 使用一个 {@link PluginClassLoader}。
 *
 * <pre>
 * plugin/mysql/jdbc-agent.plugin.mysql.jar
 * plugin/mysql/mysql-connector-java.jar
 * plugin/postgresql/postgresql.jar
 * </pre>
 *
 * 目录由 -Dja.plugins 指定。未指定时用 -Dja.home/plugin，再退回到配置文件上一级的 plugin。
 */
public final class JdbcDriverPlugins {
    private static final Logger logger = LoggerFactory.getLogger(JdbcDriverPlugins.class);

    private static final Object LOCK = new Object();
    private static final Map<String, PluginUnit> byDriverClass = new LinkedHashMap<String, PluginUnit>();
    private static final List<PluginUnit> units = new ArrayList<PluginUnit>();
    private static final List<PluginClassLoader> classLoaders = new ArrayList<PluginClassLoader>();
    private static final List<Driver> registered = new ArrayList<Driver>();
    private static final Set<String> installed = new HashSet<String>();

    private static boolean loaded;
    private static File loadedFrom;

    private JdbcDriverPlugins() {
    }

    /**
     * 按启动参数解析插件目录并装入。重复调用会先卸掉上一次的插件。
     */
    public static void loadDefault() {
        load(pluginsDirectory());
    }

    /**
     * 装入目录里的插件。目录不存在时不报错，随后只能使用已经在 server classpath 上的驱动。
     */
    public static void load(File pluginsDir) {
        synchronized (LOCK) {
            closeUnlocked();
            try {
                loadedFrom = pluginsDir;
                if (pluginsDir != null && pluginsDir.isDirectory()) {
                    logger.info("loading JDBC plugins from {}", pluginsDir.getAbsolutePath());
                    scan(pluginsDir);
                } else {
                    logger.info("JDBC plugin directory does not exist: {}", String.valueOf(pluginsDir));
                }
                loaded = true;
            } catch (RuntimeException e) {
                closeUnlocked();
                throw e;
            } catch (Exception e) {
                closeUnlocked();
                throw new IllegalStateException("failed to load JDBC plugins from " + pluginsDir, e);
            }
        }
    }

    /**
     * 按配置里的驱动类名装驱动。插件优先；插件没有时才用 server classpath，便于测试里的 H2。
     */
    public static void install(String driverClassName) {
        if (driverClassName == null || driverClassName.trim().isEmpty()) {
            throw new IllegalArgumentException("driver class name is empty");
        }
        String name = driverClassName.trim();
        synchronized (LOCK) {
            if (!loaded) {
                load(pluginsDirectory());
            }
            if (installed.contains(name)) {
                return;
            }
            PluginUnit unit = byDriverClass.get(name);
            if (unit == null) {
                if (tryLoadFromClasspath(name)) {
                    installed.add(name);
                    logger.warn("JDBC driver {} is not provided by a plugin; loaded it from the server classpath",
                            name);
                    return;
                }
                throw new IllegalStateException("no JDBC plugin provides driver " + name
                        + ". Add a plugin that declares it under " + loadedFrom);
            }
            Thread thread = Thread.currentThread();
            ClassLoader previous = thread.getContextClassLoader();
            thread.setContextClassLoader(unit.classLoader);
            try {
                Driver real = unit.plugin.loadDriver(name);
                if (real == null) {
                    throw new IllegalStateException("JDBC plugin " + unit.plugin.name()
                            + " returned no driver for " + name);
                }
                IsolatedDriver isolated = new IsolatedDriver(real);
                DriverManager.registerDriver(isolated);
                registered.add(isolated);
                installed.add(name);
                logger.info("installed JDBC driver {} from plugin {}", name, unit.plugin.name());
            } catch (SQLException e) {
                throw new IllegalStateException("failed to install JDBC driver " + name, e);
            } finally {
                thread.setContextClassLoader(previous);
            }
        }
    }

    /**
     * 返回已装插件里该驱动的类加载器。驱动来自 classpath 时返回 null。
     */
    public static ClassLoader classLoader(String driverClassName) {
        if (driverClassName == null) {
            return null;
        }
        synchronized (LOCK) {
            PluginUnit unit = byDriverClass.get(driverClassName.trim());
            return unit == null ? null : unit.classLoader;
        }
    }

    /**
     * 卸掉插件注册的驱动并关掉类加载器。
     */
    public static void close() {
        synchronized (LOCK) {
            closeUnlocked();
        }
    }

    static File pluginsDirectory() {
        String configured = System.getProperty("ja.plugins");
        if (configured != null && configured.trim().length() > 0) {
            return new File(configured.trim());
        }
        String home = System.getProperty("ja.home");
        if (home != null && home.trim().length() > 0) {
            return new File(home.trim(), "plugin");
        }
        String conf = System.getProperty("ja.conf");
        if (conf != null && !conf.startsWith("classpath:")) {
            File confFile = new File(conf);
            File confDir = confFile.getParentFile();
            if (confDir != null && confDir.getParentFile() != null) {
                return new File(confDir.getParentFile(), "plugin");
            }
        }
        return new File("plugin");
    }

    private static void scan(File dir) throws IOException {
        File[] children = dir.listFiles();
        if (children == null) {
            return;
        }
        Arrays.sort(children, new Comparator<File>() {
            @Override
            public int compare(File left, File right) {
                return left.getName().compareTo(right.getName());
            }
        });
        for (File child : children) {
            if (child.getName().startsWith(".")) {
                continue;
            }
            if (child.isDirectory()) {
                loadUnit(child.getName(), jarUrls(child));
            } else if (child.isFile() && child.getName().endsWith(".jar")) {
                loadUnit(child.getName(), new URL[]{child.toURI().toURL()});
            }
        }
    }

    private static URL[] jarUrls(File dir) throws IOException {
        File[] jars = dir.listFiles();
        if (jars == null || jars.length == 0) {
            return new URL[0];
        }
        Arrays.sort(jars, new Comparator<File>() {
            @Override
            public int compare(File left, File right) {
                return left.getName().compareTo(right.getName());
            }
        });
        List<URL> urls = new ArrayList<URL>();
        for (File jar : jars) {
            if (jar.isFile() && jar.getName().endsWith(".jar")) {
                urls.add(jar.toURI().toURL());
            }
        }
        return urls.toArray(new URL[urls.size()]);
    }

    private static void loadUnit(String unitName, URL[] urls) throws IOException {
        if (urls.length == 0) {
            logger.warn("skip JDBC plugin {}: no jar", unitName);
            return;
        }
        ClassLoader parent = JdbcDriverPlugin.class.getClassLoader();
        if (parent == null) {
            parent = ClassLoader.getSystemClassLoader();
        }
        PluginClassLoader classLoader = new PluginClassLoader(urls, parent);
        boolean keep = false;
        try {
            List<JdbcDriverPlugin> plugins = loadServices(unitName, classLoader);
            if (plugins.isEmpty()) {
                logger.warn("skip {}: META-INF/services/{} is missing",
                        unitName, JdbcDriverPlugin.class.getName());
                return;
            }
            classLoaders.add(classLoader);
            keep = true;
            for (JdbcDriverPlugin plugin : plugins) {
                register(unitName, classLoader, plugin);
            }
        } finally {
            if (!keep) {
                classLoader.close();
            }
        }
    }

    private static List<JdbcDriverPlugin> loadServices(String unitName, ClassLoader classLoader) {
        List<JdbcDriverPlugin> found = new ArrayList<JdbcDriverPlugin>();
        ServiceLoader<JdbcDriverPlugin> loader = ServiceLoader.load(JdbcDriverPlugin.class, classLoader);
        try {
            for (JdbcDriverPlugin plugin : loader) {
                found.add(plugin);
            }
        } catch (ServiceConfigurationError e) {
            throw new IllegalStateException("failed to load JDBC plugin from " + unitName, e);
        }
        return found;
    }

    private static void register(String source, PluginClassLoader classLoader, JdbcDriverPlugin plugin) {
        String[] names = plugin.driverClassNames();
        if (names == null || names.length == 0) {
            throw new IllegalStateException("JDBC plugin " + plugin.name() + " from " + source
                    + " declares no driver");
        }
        List<String> declared = new ArrayList<String>();
        for (String driverClassName : names) {
            if (driverClassName == null || driverClassName.trim().isEmpty()) {
                throw new IllegalStateException("JDBC plugin " + plugin.name() + " declares a blank driver class");
            }
            String key = driverClassName.trim();
            if (declared.contains(key)) {
                throw new IllegalStateException("JDBC plugin " + plugin.name() + " declares " + key + " more than once");
            }
            PluginUnit existing = byDriverClass.get(key);
            if (existing != null) {
                throw new IllegalStateException("JDBC driver " + key + " is provided by both plugin "
                        + existing.plugin.name() + " (" + existing.source + ") and "
                        + plugin.name() + " (" + source + ")");
            }
            declared.add(key);
        }
        PluginUnit unit = new PluginUnit(source, classLoader, plugin);
        units.add(unit);
        for (String key : declared) {
            byDriverClass.put(key, unit);
        }
        logger.info("loaded JDBC plugin {} from {} for {}", plugin.name(), source, declared);
    }

    private static boolean tryLoadFromClasspath(String name) {
        ClassLoader[] loaders = new ClassLoader[]{
                Thread.currentThread().getContextClassLoader(),
                JdbcDriverPlugins.class.getClassLoader()
        };
        for (ClassLoader loader : loaders) {
            if (loader == null) {
                continue;
            }
            try {
                Class.forName(name, true, loader);
                return true;
            } catch (ClassNotFoundException ignored) {
                // 换下一个类加载器
            }
        }
        return false;
    }

    private static void closeUnlocked() {
        Set<JdbcDriverPlugin> released = Collections.newSetFromMap(new IdentityHashMap<JdbcDriverPlugin, Boolean>());
        for (PluginUnit unit : units) {
            if (!released.add(unit.plugin)) {
                continue;
            }
            try {
                unit.plugin.release();
            } catch (Exception e) {
                logger.warn("failed to release JDBC plugin {}", unit.plugin.name(), e);
            }
        }
        for (Driver driver : registered) {
            try {
                DriverManager.deregisterDriver(driver);
            } catch (SQLException e) {
                logger.warn("failed to deregister {}", driver, e);
            }
        }
        for (URLClassLoader classLoader : classLoaders) {
            try {
                classLoader.close();
            } catch (IOException e) {
                logger.warn("failed to close JDBC plugin class loader", e);
            }
        }
        registered.clear();
        classLoaders.clear();
        units.clear();
        byDriverClass.clear();
        installed.clear();
        loaded = false;
        loadedFrom = null;
    }

    private static final class PluginUnit {
        private final String source;
        private final PluginClassLoader classLoader;
        private final JdbcDriverPlugin plugin;

        private PluginUnit(String source, PluginClassLoader classLoader, JdbcDriverPlugin plugin) {
            this.source = source;
            this.classLoader = classLoader;
            this.plugin = plugin;
        }
    }
}
