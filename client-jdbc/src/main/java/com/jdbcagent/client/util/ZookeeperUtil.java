package com.jdbcagent.client.util;

import com.jdbcagent.client.util.loadbalance.RandomLoadBalance;
import com.jdbcagent.core.util.ByteSerializer;
import com.jdbcagent.core.util.ServerRunningData;
import com.jdbcagent.core.util.ZookeeperPathUtils;
import org.I0Itec.zkclient.ZkClient;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * JDBC-Agent client 获取zk工具类
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class ZookeeperUtil {
    /**
     * 获取zk上注册的server地址
     *
     * @param zkServers zk地址
     * @param catalog   目录名
     * @return 实际server地址
     * @throws SQLException
     */
    public static String getAddressFromZk(String zkServers, String catalog) throws SQLException {
        List<ServerRunningData> serverRunningDataList = getAllServers(zkServers, catalog);

        if (serverRunningDataList.isEmpty()) {
            throw new SQLException("Empty jdbc agent server for load");
        }

        ServerRunningData selectedServer = RandomLoadBalance.doSelect(serverRunningDataList);
        return selectedServer.getAddress();

    }

    /**
     * 获取所有server地址
     *
     * @param zkServers
     * @param catalog
     * @return
     */
    public static List<ServerRunningData> getAllServers(String zkServers, String catalog) {
        ZkClient zkClient = null;
        try {
            zkClient = new ZkClient(zkServers, 3000, 3000, new ByteSerializer());
            List<String> servers = zkClient.getChildren(ZookeeperPathUtils.JA_ROOT_NODE +
                    ZookeeperPathUtils.SERVER_NODE +
                    ZookeeperPathUtils.ZOOKEEPER_SEPARATOR +
                    catalog);
            List<ServerRunningData> serverRunningDataList = new ArrayList<>();
            for (String server : servers) {
                byte[] bytes = zkClient.readData(ZookeeperPathUtils.JA_ROOT_NODE +
                        ZookeeperPathUtils.SERVER_NODE +
                        ZookeeperPathUtils.ZOOKEEPER_SEPARATOR +
                        catalog + ZookeeperPathUtils.ZOOKEEPER_SEPARATOR +
                        server, true);
                if (bytes == null) {
                    throw new SQLException("Error to load jdbc agent server");
                }
                String json = new String(bytes, StandardCharsets.UTF_8);
                ServerRunningData data = new ServerRunningData();
                int i = json.indexOf("\"address\":\"") + "\"address\":\"".length();
                int j = json.indexOf("\"", i);
                data.setAddress(json.substring(i, j));
                i = json.indexOf("\"active\":") + "\"active\":".length();
                j = json.indexOf(",\"", i);
                data.setActive("true".equals(json.substring(i, j)));
                i = json.indexOf("\"weight\":") + "\"weight\":".length();
                j = json.indexOf("}", i);
                data.setWeight(Integer.valueOf(json.substring(i, j)));
                data.setCatalog(catalog);
                if (data.isActive()) {
                    serverRunningDataList.add(data);
                }
            }
            return serverRunningDataList;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }
    }
}
