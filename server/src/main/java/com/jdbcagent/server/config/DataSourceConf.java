package com.jdbcagent.server.config;

import java.util.Map;

/**
 * JDBC-Agent server 端配置: 第三方数据源配置
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class DataSourceConf {
    private String accessUsername;                      // 暴露给client的用户名

    private String accessPassword;                      // 暴露给client的密码

    private String dsClassName;                         // 数据源类名

    private Map<String, String> dsProperties;           // 链接池参数

    private Map<String, String> writerDsProperties;     // 写数据源

    private Map<String, String> readerDsProperties;     // 读数据源

    public String getAccessUsername() {
        return accessUsername;
    }

    public void setAccessUsername(String accessUsername) {
        this.accessUsername = accessUsername;
    }

    public String getAccessPassword() {
        return accessPassword;
    }

    public void setAccessPassword(String accessPassword) {
        this.accessPassword = accessPassword;
    }

    public String getDsClassName() {
        return dsClassName;
    }

    public void setDsClassName(String dsClassName) {
        this.dsClassName = dsClassName;
    }

    public Map<String, String> getDsProperties() {
        return dsProperties;
    }

    public void setDsProperties(Map<String, String> dsProperties) {
        this.dsProperties = dsProperties;
    }

    public Map<String, String> getWriterDsProperties() {
        return writerDsProperties;
    }

    public void setWriterDsProperties(Map<String, String> writerDsProperties) {
        this.writerDsProperties = writerDsProperties;
    }

    public Map<String, String> getReaderDsProperties() {
        return readerDsProperties;
    }

    public void setReaderDsProperties(Map<String, String> readerDsProperties) {
        this.readerDsProperties = readerDsProperties;
    }
}
