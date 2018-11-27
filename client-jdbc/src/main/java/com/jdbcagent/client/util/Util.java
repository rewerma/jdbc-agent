package com.jdbcagent.client.util;

import com.jdbcagent.core.util.ServerRunningData;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * JDBC-Agent client 工具类
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class Util {
    /**
     * 解析url
     *
     * @param url url地址
     * @return 返回map对象: ip:127.0.0.1 , port:10101 catalog:test
     */
    public static Map<String, String> parseUrl(String url) throws SQLException {
        String ipPort = null;
        String catalog = null;
        if (url.startsWith("jdbc:agent:")) {
            int i = url.indexOf("/");
            if (i > -1) {
                ipPort = url.substring("jdbc:agent:".length(), i);
                catalog = url.substring(i + 1);
            }
        } else if (url.startsWith("jdbc:zookeeper:")) { //解析zk的地址
            int i = url.indexOf("/");
            if (i > -1) {
                String zkServers = url.substring("jdbc:zookeeper:".length(), i);
                catalog = url.substring(i + 1);
                ipPort = ZookeeperUtil.getAddressFromZk(zkServers, catalog);
            }
        }
        if (ipPort == null) {
            throw new RuntimeException("error jdbc url. ");
        }

        String[] ip_port = ipPort.split(":");
        Map<String, String> res = new HashMap<>();
        res.put("ip", ip_port[0]);
        res.put("port", ip_port[1]);
        res.put("catalog", catalog);
        return res;
    }

    /**
     * 解析zk的地址和catalog
     *
     * @param url zk url
     * @return 返回map对象: zkServers:127.0.0.1:2181 catalog:test
     */
    public static Map<String, String> parseZkUrl(String url) {
        if (url.startsWith("jdbc:zookeeper:")) { //解析zk的地址
            int i = url.indexOf("/");
            if (i > -1) {
                String zkServers = url.substring("jdbc:zookeeper:".length(), i);
                String catalog = url.substring(i + 1);
                Map<String, String> res = new HashMap<>();
                res.put("zkServers", zkServers);
                res.put("catalog", catalog);
                return res;
            }
        }
        return null;
    }

    /**
     * 解析json为serverRunningData对象
     *
     * @param json json字符串
     * @return serverRunningData对象
     */
    public static ServerRunningData parseJson(String json) {
        ServerRunningData data = new ServerRunningData();
        int i = json.indexOf("\"address\":\"") + "\"address\":\"".length();
        int j = json.indexOf("\"", i);
        data.setAddress(json.substring(i, j));
        i = json.indexOf("\"catalog\":\"") + "\"catalog\":\"".length();
        j = json.indexOf("\"", i);
        data.setCatalog(json.substring(i, j));
        i = json.indexOf("\"active\":") + "\"active\":".length();
        j = json.indexOf(",\"", i);
        data.setActive("true".equals(json.substring(i, j)));
        i = json.indexOf("\"weight\":") + "\"weight\":".length();
        j = json.indexOf("}", i);
        data.setWeight(Integer.valueOf(json.substring(i, j)));
        return data;
    }
}
