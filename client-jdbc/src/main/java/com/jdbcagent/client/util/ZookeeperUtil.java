package com.jdbcagent.client.util;

import com.jdbcagent.client.util.loadbalance.RoundRobinLoadBalance;
import com.jdbcagent.core.util.ByteSerializer;
import com.jdbcagent.core.util.ServerRunningData;
import com.jdbcagent.core.util.ZookeeperPathUtils;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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

        ServerRunningData selectedServer = RoundRobinLoadBalance.doSelect(serverRunningDataList);
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
                ServerRunningData data = Util.parseJson(json);
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

    private ZkClient zkClient;

    public ZookeeperUtil(String zkServers) {
        zkClient = new ZkClient(zkServers, 3000, 3000, new ByteSerializer());
    }

    public void close() {
        if (zkClient != null) {
            zkClient.close();
        }
    }

    public void listenData(String path, final Set<String> beforeChilds, final DataChangeListener dataChangeListener) {
        IZkDataListener dataListener = new IZkDataListener() {
            @Override
            public void handleDataChange(String dataPath, Object data) throws Exception {
                String json = new String((byte[]) data, StandardCharsets.UTF_8);
                ServerRunningData serverRunningData = Util.parseJson(json);
                if (serverRunningData.isActive()) {
                    if (!beforeChilds.contains(serverRunningData.getAddress())) {
                        // 新增 server
                        dataChangeListener.onAdd(serverRunningData);
                    }
                } else {
                    if (beforeChilds.contains(serverRunningData.getAddress())) {
                        // 删除 server
                        dataChangeListener.onRemove(serverRunningData.getAddress());
                    }
                }
            }

            @Override
            public void handleDataDeleted(String dataPath) throws Exception {
            }
        };

        zkClient.subscribeDataChanges(path, dataListener);
    }

    public void listenChilds(String parentPath, final Set<String> beforeChilds, final DataChangeListener dataChangeListener) {
        IZkChildListener childListener = new IZkChildListener() {
            @Override
            public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                for (String currentChild : currentChilds) {
                    if (!beforeChilds.contains(currentChild)) {
                        // 新增 server
                        byte[] bytes = zkClient.readData(parentPath + ZookeeperPathUtils.ZOOKEEPER_SEPARATOR +
                                currentChild, true);
                        String json = new String(bytes, StandardCharsets.UTF_8);
                        ServerRunningData serverRunningData = Util.parseJson(json);
                        dataChangeListener.onAdd(serverRunningData);
                    }
                }
                for (String beforeChild : beforeChilds) {
                    if (!currentChilds.contains(beforeChild)) {
                        // 删除 server
                        dataChangeListener.onRemove(beforeChild);
                    }
                }
            }
        };

        zkClient.subscribeChildChanges(parentPath, childListener);
    }

    public interface DataChangeListener {
        void onAdd(ServerRunningData serverRunningData);

        void onRemove(String key);
    }
}
