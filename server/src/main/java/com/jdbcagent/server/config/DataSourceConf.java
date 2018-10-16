package com.jdbcagent.server.config;

import java.util.Map;

/**
 * JDBC-Agent server 端配置: 第三方数据源配置
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class DataSourceConf {
    private String accessUsername;              // 暴露给client的用户名

    private String accessPassword;              // 暴露给client的密码

    private String cpClassName;                 // 链接池类名

    private Map<String, String> cpProperties;   // 链接池参数

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

    public String getCpClassName() {
        return cpClassName;
    }

    public void setCpClassName(String cpClassName) {
        this.cpClassName = cpClassName;
    }

    public Map<String, String> getCpProperties() {
        return cpProperties;
    }

    public void setCpProperties(Map<String, String> cpProperties) {
        this.cpProperties = cpProperties;
    }
}
