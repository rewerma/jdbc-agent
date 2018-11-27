package com.jdbcagent.server.config;

import com.jdbcagent.server.ServerLauncher;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * JDBC-Agent server 配置解析工具类
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class Configuration {
    private static JdbcAgentConf jdbcAgentConf;

    public static JdbcAgentConf getJdbcAgentCon() {
        if (jdbcAgentConf == null) {
            throw new RuntimeException("jdbcAgentConf is not initialed");
        }
        return jdbcAgentConf;
    }

    public static void setJdbcAgentConf(JdbcAgentConf jdbcAgentConf) {
        Configuration.jdbcAgentConf = jdbcAgentConf;
    }

    public static JdbcAgentConf parse(InputStream in) {
        jdbcAgentConf = new Yaml().loadAs(in, JdbcAgentConf.class);
        return jdbcAgentConf;
    }

    private static Map<String, String> loadDsConfigs() {
        Map<String, String> configContentMap = new HashMap<>();

        File configDir = new File(".." + File.separator + "conf" + File.separator + "datasources");
        if (!configDir.exists()) {
            URL url = ServerLauncher.class.getClassLoader().getResource("");
            if (url != null) {
                configDir = new File(url.getPath() + File.separator + "datasources");
            }
        }

        File[] files = configDir.listFiles();
        if (files != null) {
            for (File file : files) {
                String fileName = file.getName();
                if (!fileName.endsWith(".properties")) {
                    continue;
                }
                try (InputStream in = new FileInputStream(file)) {
                    byte[] bytes = new byte[in.available()];
                    in.read(bytes);
                    String configContent = new String(bytes, StandardCharsets.UTF_8);
                    configContentMap.put(fileName, configContent);
                } catch (IOException e) {
                    throw new RuntimeException("Read config: " + fileName + " error. ", e);
                }
            }
        }

        return configContentMap;
    }

    public static JdbcAgentConf loadConf(InputStream in) throws IOException {
        Properties config = new Properties();
        config.load(in);
        in.close();
        JdbcAgentConf jdbcAgentConf = new JdbcAgentConf();
        JdbcAgentConf.JdbcAgent jdbcAgent = new JdbcAgentConf.JdbcAgent();
        jdbcAgentConf.setJdbcAgent(jdbcAgent);
        jdbcAgent.setZkServers(config.getProperty("jdbc.agent.zkServers"));
        jdbcAgent.setIp(config.getProperty("jdbc.agent.ip", "127.0.0.1"));
        jdbcAgent.setPort(Integer.valueOf(config.getProperty("jdbc.agent.port", "10101")));
        jdbcAgent.setSerialize(config.getProperty("jdbc.agent.serialize"));
        List<JdbcAgentConf.Catalog> catalogs = jdbcAgent.getCatalogs();
        if (catalogs == null) {
            catalogs = new ArrayList<>();
            jdbcAgent.setCatalogs(catalogs);
        }
        setJdbcAgentConf(jdbcAgentConf);

        return jdbcAgentConf;
    }

    public static void loadDS() throws IOException {
        List<JdbcAgentConf.Catalog> catalogs = jdbcAgentConf.getJdbcAgent().getCatalogs();
        Map<String, String> configs = loadDsConfigs();
        for (String dsConf : configs.values()) {
            Properties dsConfig = new Properties();
            Reader reader = new StringReader(dsConf);
            dsConfig.load(reader);
            reader.close();
            String catalog = dsConfig.getProperty("jdbc.agent.catalog");
            JdbcAgentConf.Catalog catalogConf = null;
            for (JdbcAgentConf.Catalog catalogConfTmp : catalogs) {
                if (catalogConfTmp.getCatalog().equals(catalog)) {
                    catalogConf = catalogConfTmp;
                    break;
                }
            }
            if (catalogConf == null) {
                catalogConf = new JdbcAgentConf.Catalog();
                catalogs.add(catalogConf);
            }
            catalogConf.setCatalog(catalog);

            List<DataSourceConf> dataSourceConfs = catalogConf.getDataSources();
            if (dataSourceConfs == null) {
                dataSourceConfs = new ArrayList<>();
                catalogConf.setDataSources(dataSourceConfs);
            }

            DataSourceConf dataSourceConf = new DataSourceConf();
            dataSourceConfs.add(dataSourceConf);
            dataSourceConf.setAccessUsername(dsConfig.getProperty("jdbc.agent.accessUsername"));
            dataSourceConf.setAccessPassword(dsConfig.getProperty("jdbc.agent.accessPassword"));
            dataSourceConf.setDsClassName(dsConfig.getProperty("jdbc.agent.dsClassName"));

            int size = "jdbc.agent.dsProperties.".length();
            for (Map.Entry<Object, Object> entry : dsConfig.entrySet()) {
                String key = (String) entry.getKey();
                String val = (String) entry.getValue();
                if (key.startsWith("jdbc.agent.dsProperties")) {
                    Map<String, String> dsProperties = dataSourceConf.getDsProperties();
                    if (dsProperties == null) {
                        dsProperties = new LinkedHashMap<>();
                        dataSourceConf.setDsProperties(dsProperties);
                    }
                    key = key.substring(size);
                    dsProperties.put(key, val);
                }
            }
        }

    }
}
