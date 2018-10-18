package com.jdbcagent.server.config;

import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;

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

    public static JdbcAgentConf parse(InputStream in) {
        jdbcAgentConf = new Yaml().loadAs(in, JdbcAgentConf.class);
        return jdbcAgentConf;
    }
}
