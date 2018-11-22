package com.jdbcagent.client.uitl;

import com.jdbcagent.core.util.ByteSerializer;
import com.jdbcagent.core.util.ZookeeperPathUtils;
import org.I0Itec.zkclient.ZkClient;

import java.sql.SQLException;

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
        ZkClient zkClient = null;
        try {
            zkClient = new ZkClient(zkServers, 3000, 3000, new ByteSerializer());
            byte[] bytes = zkClient.readData(ZookeeperPathUtils.JA_ROOT_NODE +
                    ZookeeperPathUtils.ZOOKEEPER_SEPARATOR +
                    catalog + ZookeeperPathUtils.ZOOKEEPER_SEPARATOR +
                    ZookeeperPathUtils.RUNNING_NODE, true);
            if (bytes == null) {
                throw new SQLException("Error to connect jdbc agent server");
            }
            String json = new String(bytes, "UTF-8");
            int i = json.indexOf("\"address\":\"") + "\"address\":\"".length();
            int j = json.indexOf("\"", i);
            return json.substring(i, j);
        } catch (Exception e) {
            throw new SQLException(e);
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }
    }
}
