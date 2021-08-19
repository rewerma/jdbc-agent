package com.jdbcagent.server.config;

import java.util.Map;

/**
 * JDBC-Agent server 端配置: 第三方数据源配置
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class DataSourceConf {
    private String accessUsername;                          // 暴露给client的用户名
    private String accessPassword;                          // 暴露给client的密码

    private Map<String, Object> dsManager;                  // 普通dataSource链接配置

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

    public Map<String, Object> getDsManager() {
        return dsManager;
    }

    public void setDsManager(Map<String, Object> dsManager) {
        this.dsManager = dsManager;
    }

    public DruidDataSourceConf getDruid() {
        return druid;
    }

    public void setDruid(DruidDataSourceConf druid) {
        this.druid = druid;
    }
}
