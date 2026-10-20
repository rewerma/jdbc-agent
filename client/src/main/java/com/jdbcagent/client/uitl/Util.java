package com.jdbcagent.client.uitl;

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
     * @return
     */
    public static Map<String, String> parseUrl(String url) throws SQLException {
        String ipPort = null;
        String catalog = null;
        String zkServers = null;
        if (url.startsWith("jdbc:agent:")) {
            int i = url.indexOf("/");
            if (i > -1) {
                ipPort = url.substring("jdbc:agent:".length(), i);
                catalog = url.substring(i + 1);
            }
        } else if (url.startsWith("jdbc:zookeeper:")) { //解析zk的地址
            int i = url.indexOf("/");
            if (i > -1) {
                zkServers = url.substring("jdbc:zookeeper:".length(), i);
                catalog = url.substring(i + 1);
                ipPort = ZookeeperUtil.getAddressFromZk(zkServers, catalog);
            }
        }
        if (ipPort == null) {
            throw new RuntimeException("error jdbc url. ");
        }

        int colon = ipPort.lastIndexOf(':');
        if (colon <= 0 || colon == ipPort.length() - 1) {
            throw new SQLException("illegal jdbc agent address: " + ipPort);
        }
        Map<String, String> res = new HashMap<>();
        res.put("ip", ipPort.substring(0, colon));
        res.put("port", ipPort.substring(colon + 1));
        res.put("catalog", catalog);
        if (zkServers != null) {
            res.put("zkServers", zkServers);
        }
        return res;
    }
}
