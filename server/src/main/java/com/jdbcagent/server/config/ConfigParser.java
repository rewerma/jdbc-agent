package com.jdbcagent.server.config;

import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;

/**
 * JDBC-Agent server 配置解析工具类
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class ConfigParser {
    public static JdbcAgentConf parse(InputStream in) {
        return new Yaml().loadAs(in, JdbcAgentConf.class);
    }
}
