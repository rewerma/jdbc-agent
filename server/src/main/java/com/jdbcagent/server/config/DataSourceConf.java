package com.jdbcagent.server.config;

/**
 * JDBC-Agent server 端配置: 第三方数据源配置
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class DataSourceConf {
    private String accessUsername;                          // 暴露给client的用户名
    private String accessPassword;                          // 暴露给client的密码

    private DruidDataSourceConf druid;                      // druid 链接池配置

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

    public DruidDataSourceConf getDruid() {
        return druid;
    }

    public void setDruid(DruidDataSourceConf druid) {
        this.druid = druid;
    }
}
