package com.jdbcagent.server.plugin;

import com.jdbcagent.server.config.DataSourceConf;
import com.jdbcagent.server.datasources.DataSourceFactory;
import org.junit.Assert;
import org.junit.Test;

import javax.sql.DataSource;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

public class JdbcDriverPluginsTest {

    @Test
    public void pluginsUseIsolatedClassLoaders() throws Exception {
        File root = workspace("isolated");
        compileDriverPlugin(new File(root, "a"), "PluginA", "DriverA", "jdbc:iso-a:", "A");
        compileDriverPlugin(new File(root, "b"), "PluginB", "DriverB", "jdbc:iso-b:", "B");
        try {
            JdbcDriverPlugins.load(root);
            JdbcDriverPlugins.install("fixture.DriverA");
            JdbcDriverPlugins.install("fixture.DriverB");

            ClassLoader loaderA = JdbcDriverPlugins.classLoader("fixture.DriverA");
            ClassLoader loaderB = JdbcDriverPlugins.classLoader("fixture.DriverB");
            Assert.assertNotSame(loaderA, loaderB);
            Assert.assertTrue(loaderA instanceof PluginClassLoader);
            Assert.assertTrue(loaderB instanceof PluginClassLoader);

            Class<?> widgetA = Class.forName("isolated.shared.Widget", false, loaderA);
            Class<?> widgetB = Class.forName("isolated.shared.Widget", false, loaderB);
            Class<?> parentWidget = Class.forName("isolated.shared.Widget");
            Assert.assertNotSame(widgetA, widgetB);
            Assert.assertNotSame(parentWidget, widgetA);
            Assert.assertEquals("A", widgetA.getField("VALUE").get(null));
            Assert.assertEquals("B", widgetB.getField("VALUE").get(null));
            Assert.assertEquals("parent", parentWidget.getField("VALUE").get(null));
            Assert.assertSame(loaderA, widgetA.getClassLoader());

            try {
                Class.forName("fixture.DriverB", false, loaderA);
                Assert.fail("plugin A loaded fixture.DriverB");
            } catch (ClassNotFoundException expected) {
            }
            try {
                Class.forName("fixture.DriverA");
                Assert.fail("server classpath loaded fixture.DriverA");
            } catch (ClassNotFoundException expected) {
            }

            Class<?> driverClass = Class.forName("fixture.DriverA", false, loaderA);
            Assert.assertEquals(Integer.valueOf(1), driverClass.getMethod("registeredVisible").invoke(null));

            Connection connection = DriverManager.getConnection("jdbc:iso-a:mem");
            connection.close();
            Assert.assertEquals(1, widgetA.getField("hits").getInt(null));
            Assert.assertEquals(0, widgetB.getField("hits").getInt(null));
            Assert.assertEquals(0, parentWidget.getField("hits").getInt(null));

            JdbcDriverPlugins.close();
            Assert.assertEquals(Integer.valueOf(0), driverClass.getMethod("registeredVisible").invoke(null));
        } finally {
            JdbcDriverPlugins.close();
        }
    }

    @Test
    public void duplicateDriverClassIsRejected() throws Exception {
        File root = workspace("duplicate");
        compileNameOnlyPlugin(new File(root, "a"), "PluginA", "a", "fixture.Shared");
        compileNameOnlyPlugin(new File(root, "b"), "PluginB", "b", "fixture.Shared");
        try {
            JdbcDriverPlugins.load(root);
            Assert.fail("two plugins declared fixture.Shared");
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("fixture.Shared"));
            Assert.assertTrue(e.getMessage().contains("both plugin"));
        } finally {
            JdbcDriverPlugins.close();
        }
    }

    @Test
    public void jarWithoutServiceIsSkipped() throws Exception {
        File root = workspace("noservice");
        File jar = new File(root, "notes.jar");
        jar.getParentFile().mkdirs();
        JarOutputStream out = new JarOutputStream(new FileOutputStream(jar));
        try {
            out.putNextEntry(new JarEntry("META-INF/README"));
            out.write("not a plugin\n".getBytes("UTF-8"));
            out.closeEntry();
        } finally {
            out.close();
        }
        try {
            JdbcDriverPlugins.load(root);
            try {
                JdbcDriverPlugins.install("com.missing.Driver");
                Assert.fail("missing driver was installed");
            } catch (IllegalStateException e) {
                Assert.assertTrue(e.getMessage().contains("com.missing.Driver"));
            }
        } finally {
            JdbcDriverPlugins.close();
        }
    }

    @Test
    public void classpathDriverIsUsedWhenNoPluginProvidesIt() throws Exception {
        File root = workspace("classpath");
        Assert.assertTrue(new File(root, "empty").mkdirs());
        try {
            JdbcDriverPlugins.load(new File(root, "empty"));
            DataSourceConf conf = new DataSourceConf();
            Map<String, Object> dsManager = new HashMap<String, Object>();
            dsManager.put("driverClass", "org.h2.Driver");
            dsManager.put("jdbcUrl", "jdbc:h2:mem:plugin_fallback");
            dsManager.put("username", "sa");
            dsManager.put("password", "");
            conf.setDsManager(dsManager);

            DataSource dataSource = DataSourceFactory.getDataSource(conf);
            Connection connection = dataSource.getConnection();
            connection.close();
        } finally {
            JdbcDriverPlugins.close();
        }
    }

    @Test
    public void serverClasspathDoesNotContainJdbcDrivers() {
        assertAbsent("com.mysql.cj.jdbc.Driver");
        assertAbsent("com.mysql.jdbc.Driver");
        assertAbsent("org.postgresql.Driver");
    }

    @Test
    public void pluginDirectoryFollowsSystemProperties() {
        String plugins = System.getProperty("ja.plugins");
        String home = System.getProperty("ja.home");
        String conf = System.getProperty("ja.conf");
        try {
            System.setProperty("ja.plugins", "/tmp/custom-plugins");
            Assert.assertEquals(new File("/tmp/custom-plugins"), JdbcDriverPlugins.pluginsDirectory());

            System.clearProperty("ja.plugins");
            System.setProperty("ja.home", "/tmp/ja-home");
            Assert.assertEquals(new File("/tmp/ja-home/plugin"), JdbcDriverPlugins.pluginsDirectory());

            System.clearProperty("ja.home");
            System.setProperty("ja.conf", "/opt/jdbc-agent/conf/jdbc-agent.yml");
            Assert.assertEquals(new File("/opt/jdbc-agent/plugin"), JdbcDriverPlugins.pluginsDirectory());

            System.setProperty("ja.conf", "classpath:jdbc-agent.yml");
            Assert.assertEquals(new File("plugin"), JdbcDriverPlugins.pluginsDirectory());
        } finally {
            restore("ja.plugins", plugins);
            restore("ja.home", home);
            restore("ja.conf", conf);
        }
    }

    private static void assertAbsent(String className) {
        try {
            Class.forName(className);
            Assert.fail("server classpath contains " + className);
        } catch (ClassNotFoundException expected) {
        }
    }

    private static void restore(String key, String value) {
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
    }

    private static File workspace(String name) {
        File root = new File("target/test-plugins/" + name + "-" + System.nanoTime());
        Assert.assertTrue(root.mkdirs());
        return root;
    }

    private static void compileDriverPlugin(File dir, String pluginSimple, String driverSimple,
                                            String urlPrefix, String marker) throws Exception {
        File sources = new File(dir, "src");
        File classes = new File(dir, "classes");
        write(new File(sources, "fixture/" + pluginSimple + ".java"), pluginSource(pluginSimple, driverSimple));
        write(new File(sources, "fixture/" + driverSimple + ".java"),
                driverSource(pluginSimple, driverSimple, urlPrefix, marker));
        write(new File(sources, "isolated/shared/Widget.java"), widgetSource(marker));
        write(new File(sources, "isolated/Marker.java"), markerSource(marker));
        compile(classes,
                new File(sources, "fixture/" + pluginSimple + ".java"),
                new File(sources, "fixture/" + driverSimple + ".java"),
                new File(sources, "isolated/shared/Widget.java"),
                new File(sources, "isolated/Marker.java"));
        jar(classes, new File(dir, "plugin.jar"), "fixture." + pluginSimple);
    }

    private static void compileNameOnlyPlugin(File dir, String pluginSimple, String pluginName,
                                              String driverClass) throws Exception {
        File sources = new File(dir, "src");
        File classes = new File(dir, "classes");
        write(new File(sources, "fixture/" + pluginSimple + ".java"),
                nameOnlySource(pluginSimple, pluginName, driverClass));
        compile(classes, new File(sources, "fixture/" + pluginSimple + ".java"));
        jar(classes, new File(dir, "plugin.jar"), "fixture." + pluginSimple);
    }

    private static void compile(File classes, File... sources) throws Exception {
        Assert.assertTrue(classes.mkdirs());
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null) {
            throw new IllegalStateException("JDK compiler is required to build plugin fixtures");
        }
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();
        StandardJavaFileManager fileManager = compiler.getStandardFileManager(
                diagnostics, null, Charset.forName("UTF-8"));
        try {
            Iterable<? extends JavaFileObject> units = fileManager.getJavaFileObjectsFromFiles(Arrays.asList(sources));
            List<String> options = Arrays.asList(
                    "-encoding", "UTF-8",
                    "-source", "8",
                    "-target", "8",
                    "-classpath", System.getProperty("java.class.path"),
                    "-d", classes.getAbsolutePath());
            Boolean ok = compiler.getTask(null, fileManager, diagnostics, options, null, units).call();
            if (!Boolean.TRUE.equals(ok)) {
                StringBuilder message = new StringBuilder();
                for (Diagnostic<? extends JavaFileObject> diagnostic : diagnostics.getDiagnostics()) {
                    message.append(diagnostic).append('\n');
                }
                throw new IllegalStateException(message.toString());
            }
        } finally {
            fileManager.close();
        }
    }

    private static void jar(File classes, File jarFile, String serviceImpl) throws IOException {
        jarFile.getParentFile().mkdirs();
        JarOutputStream out = new JarOutputStream(new FileOutputStream(jarFile));
        try {
            addClassFiles(out, classes, "");
            out.putNextEntry(new JarEntry("META-INF/"));
            out.closeEntry();
            out.putNextEntry(new JarEntry("META-INF/services/"));
            out.closeEntry();
            out.putNextEntry(new JarEntry("META-INF/services/com.jdbcagent.plugin.JdbcDriverPlugin"));
            out.write((serviceImpl + "\n").getBytes("UTF-8"));
            out.closeEntry();
        } finally {
            out.close();
        }
    }

    private static void addClassFiles(JarOutputStream out, File dir, String prefix) throws IOException {
        File[] files = dir.listFiles();
        if (files == null) {
            return;
        }
        for (File file : files) {
            String name = prefix.isEmpty() ? file.getName() : prefix + "/" + file.getName();
            if (file.isDirectory()) {
                addClassFiles(out, file, name);
            } else if (file.getName().endsWith(".class")) {
                out.putNextEntry(new JarEntry(name));
                copy(file, out);
                out.closeEntry();
            }
        }
    }

    private static void copy(File file, OutputStream out) throws IOException {
        InputStream in = new FileInputStream(file);
        try {
            byte[] buffer = new byte[4096];
            int read;
            while ((read = in.read(buffer)) >= 0) {
                out.write(buffer, 0, read);
            }
        } finally {
            in.close();
        }
    }

    private static void write(File file, String source) throws IOException {
        file.getParentFile().mkdirs();
        Writer writer = new OutputStreamWriter(new FileOutputStream(file), "UTF-8");
        try {
            writer.write(source);
        } finally {
            writer.close();
        }
    }

    private static String pluginSource(String pluginSimple, String driverSimple) {
        return ""
                + "package fixture;\n"
                + "import com.jdbcagent.plugin.JdbcDriverPlugin;\n"
                + "import java.sql.Driver;\n"
                + "import java.sql.DriverManager;\n"
                + "import java.sql.SQLException;\n"
                + "import java.util.ArrayList;\n"
                + "import java.util.Enumeration;\n"
                + "import java.util.List;\n"
                + "public class " + pluginSimple + " implements JdbcDriverPlugin {\n"
                + "  public String name() { return \"" + pluginSimple + "\"; }\n"
                + "  public String[] driverClassNames() { return new String[]{\"fixture." + driverSimple + "\"}; }\n"
                + "  public Driver loadDriver(String driverClassName) throws SQLException {\n"
                + "    try {\n"
                + "      Class type = Class.forName(driverClassName, true, " + pluginSimple + ".class.getClassLoader());\n"
                + "      return (Driver) type.getDeclaredConstructor().newInstance();\n"
                + "    } catch (Exception e) {\n"
                + "      throw new SQLException(e);\n"
                + "    }\n"
                + "  }\n"
                + "  public void release() throws SQLException {\n"
                + "    ClassLoader mine = " + pluginSimple + ".class.getClassLoader();\n"
                + "    Enumeration drivers = DriverManager.getDrivers();\n"
                + "    List owned = new ArrayList();\n"
                + "    while (drivers.hasMoreElements()) {\n"
                + "      Driver driver = (Driver) drivers.nextElement();\n"
                + "      if (\"fixture." + driverSimple + "\".equals(driver.getClass().getName())\n"
                + "          && driver.getClass().getClassLoader() == mine) {\n"
                + "        owned.add(driver);\n"
                + "      }\n"
                + "    }\n"
                + "    for (int i = 0; i < owned.size(); i++) {\n"
                + "      DriverManager.deregisterDriver((Driver) owned.get(i));\n"
                + "    }\n"
                + "  }\n"
                + "}\n";
    }

    private static String driverSource(String pluginSimple, String driverSimple, String urlPrefix, String marker) {
        return ""
                + "package fixture;\n"
                + "import java.lang.reflect.InvocationHandler;\n"
                + "import java.lang.reflect.Method;\n"
                + "import java.lang.reflect.Proxy;\n"
                + "import java.sql.Connection;\n"
                + "import java.sql.Driver;\n"
                + "import java.sql.DriverManager;\n"
                + "import java.sql.DriverPropertyInfo;\n"
                + "import java.sql.SQLException;\n"
                + "import java.sql.SQLFeatureNotSupportedException;\n"
                + "import java.util.Enumeration;\n"
                + "import java.util.Properties;\n"
                + "import java.util.logging.Logger;\n"
                + "public class " + driverSimple + " implements Driver {\n"
                + "  static {\n"
                + "    try {\n"
                + "      DriverManager.registerDriver(new " + driverSimple + "());\n"
                + "    } catch (SQLException e) {\n"
                + "      throw new ExceptionInInitializerError(e);\n"
                + "    }\n"
                + "  }\n"
                + "  public static int registeredVisible() {\n"
                + "    int count = 0;\n"
                + "    Enumeration drivers = DriverManager.getDrivers();\n"
                + "    while (drivers.hasMoreElements()) {\n"
                + "      Driver driver = (Driver) drivers.nextElement();\n"
                + "      if (driver.getClass() == " + driverSimple + ".class) {\n"
                + "        count++;\n"
                + "      }\n"
                + "    }\n"
                + "    return count;\n"
                + "  }\n"
                + "  public Connection connect(String url, Properties info) throws SQLException {\n"
                + "    if (!acceptsURL(url)) {\n"
                + "      return null;\n"
                + "    }\n"
                + "    try {\n"
                + "      Class widget = Class.forName(\"isolated.shared.Widget\");\n"
                + "      if (!\"" + marker + "\".equals(widget.getField(\"VALUE\").get(null))) {\n"
                + "        throw new SQLException(\"widget \" + widget.getField(\"VALUE\").get(null));\n"
                + "      }\n"
                + "      widget.getField(\"hits\").setInt(null, widget.getField(\"hits\").getInt(null) + 1);\n"
                + "      ClassLoader context = Thread.currentThread().getContextClassLoader();\n"
                + "      Class markerClass = Class.forName(\"isolated.Marker\", false, context);\n"
                + "      if (!\"" + marker + "\".equals(markerClass.getField(\"VALUE\").get(null))) {\n"
                + "        throw new SQLException(\"marker \" + markerClass.getField(\"VALUE\").get(null));\n"
                + "      }\n"
                + "    } catch (SQLException e) {\n"
                + "      throw e;\n"
                + "    } catch (Exception e) {\n"
                + "      throw new SQLException(\"isolated class loader check failed\", e);\n"
                + "    }\n"
                + "    return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),\n"
                + "        new Class[]{Connection.class}, new InvocationHandler() {\n"
                + "          public Object invoke(Object proxy, Method method, Object[] args) {\n"
                + "            Class type = method.getReturnType();\n"
                + "            if (type == boolean.class) return Boolean.FALSE;\n"
                + "            if (type == int.class) return Integer.valueOf(0);\n"
                + "            if (type == long.class) return Long.valueOf(0L);\n"
                + "            return null;\n"
                + "          }\n"
                + "        });\n"
                + "  }\n"
                + "  public boolean acceptsURL(String url) {\n"
                + "    return url != null && url.startsWith(\"" + urlPrefix + "\");\n"
                + "  }\n"
                + "  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {\n"
                + "    return new DriverPropertyInfo[0];\n"
                + "  }\n"
                + "  public int getMajorVersion() { return 1; }\n"
                + "  public int getMinorVersion() { return 0; }\n"
                + "  public boolean jdbcCompliant() { return false; }\n"
                + "  public Logger getParentLogger() throws SQLFeatureNotSupportedException {\n"
                + "    throw new SQLFeatureNotSupportedException();\n"
                + "  }\n"
                + "}\n";
    }

    private static String widgetSource(String marker) {
        return "package isolated.shared;\n"
                + "public class Widget {\n"
                + "  public static final String VALUE = \"" + marker + "\";\n"
                + "  public static int hits;\n"
                + "}\n";
    }

    private static String markerSource(String marker) {
        return "package isolated;\n"
                + "public class Marker {\n"
                + "  public static final String VALUE = \"" + marker + "\";\n"
                + "}\n";
    }

    private static String nameOnlySource(String pluginSimple, String pluginName, String driverClass) {
        return ""
                + "package fixture;\n"
                + "import com.jdbcagent.plugin.JdbcDriverPlugin;\n"
                + "import java.sql.Driver;\n"
                + "import java.sql.SQLException;\n"
                + "public class " + pluginSimple + " implements JdbcDriverPlugin {\n"
                + "  public String name() { return \"" + pluginName + "\"; }\n"
                + "  public String[] driverClassNames() { return new String[]{\"" + driverClass + "\"}; }\n"
                + "  public Driver loadDriver(String driverClassName) throws SQLException {\n"
                + "    throw new SQLException(driverClassName);\n"
                + "  }\n"
                + "  public void release() {\n"
                + "  }\n"
                + "}\n";
    }
}
