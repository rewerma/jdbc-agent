package com.jdbcagent.core.util;

/**
 * JDBC-Agent server 运行的server信息
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class ServerRunningData {
    private String catalog;         // 目录名
    private String address;         // server地址
    private boolean active = true;  // 是否活动
    private int weight = 100;       // 权重

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }
}
