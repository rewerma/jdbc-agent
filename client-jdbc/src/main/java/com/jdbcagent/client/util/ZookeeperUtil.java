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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * JDBC-Agent client 获取zk工具类
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class ZookeeperUtil {
    private ZkClient zkClient;                              // zookeeper client
    private ScheduledExecutorService executorService;       // 定时扫描zk线程池

    /**
     * 构造方法
     *
     * @param zkServers zk地址
     */
    public ZookeeperUtil(String zkServers) {
        zkClient = new ZkClient(zkServers, 3000, 3000, new ByteSerializer());
        executorService = Executors.newScheduledThreadPool(1000);
    }

    /**
     * 关闭操作
     */
    public void close() {
        if (zkClient != null) {
            zkClient.close();
        }
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    /**
     * 监听节点内容修改
     *
     * @param path               节点路径
     * @param beforeChildren     修改前的所有服务列表
     * @param dataChangeListener 监听器
     */
    public void listenData(String path, final Set<String> beforeChildren, final DataChangeListener dataChangeListener) {
        IZkDataListener dataListener = new IZkDataListener() {
            @Override
            public void handleDataChange(String dataPath, Object data) throws Exception {
                String json = new String((byte[]) data, StandardCharsets.UTF_8);
                ServerRunningData serverRunningData = Util.parseJson(json);
                if (serverRunningData.isActive()) {
                    if (!beforeChildren.contains(serverRunningData.getAddress())) {
                        // 新增 server
                        dataChangeListener.onAdd(serverRunningData);
                    }
                } else {
                    if (beforeChildren.contains(serverRunningData.getAddress())) {
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

    /**
     * 监听节点变化
     *
     * @param parentPath         节点路径
     * @param beforeChildren     修改前的所有服务列表
     * @param dataChangeListener 监听器
     */
    public void listenChildren(String parentPath, final Set<String> beforeChildren, final DataChangeListener dataChangeListener) {
        IZkChildListener childListener = new IZkChildListener() {
            @Override
            public void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {
                dataOperation(parentPath, currentChildren, beforeChildren, dataChangeListener);
            }
        };

        zkClient.subscribeChildChanges(parentPath, childListener);
    }

    /**
     * 定时扫描
     *
     * @param parentPath         节点路径
     * @param interval           扫描时间间隔
     * @param beforeChildren     修改前的所有服务列表
     * @param dataChangeListener 监听器
     */
    public void scheduleScan(final String parentPath, int interval,
                             final Set<String> beforeChildren, final DataChangeListener dataChangeListener) {
        executorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                List<String> children = zkClient.getChildren(parentPath);
                dataOperation(parentPath, children, beforeChildren, dataChangeListener);
            }
        }, 5, interval, TimeUnit.SECONDS);
    }

    /**
     * 节点变化操作
     *
     * @param parentPath         节点路径
     * @param children           子节点
     * @param beforeChilds       修改前的所有服务列表
     * @param dataChangeListener 监听器
     */
    private void dataOperation(String parentPath, List<String> children, Set<String> beforeChilds, DataChangeListener dataChangeListener) {
        if (children != null) {
            for (String child : children) {
                if (!beforeChilds.contains(child)) {
                    // 新增
                    byte[] bytes = zkClient.readData(parentPath + ZookeeperPathUtils.ZOOKEEPER_SEPARATOR +
                            child, true);
                    String json = new String(bytes, StandardCharsets.UTF_8);
                    ServerRunningData serverRunningData = Util.parseJson(json);
                    dataChangeListener.onAdd(serverRunningData);
                }
            }
            for (String beforeChild : beforeChilds) {
                if (!children.contains(beforeChild)) {
                    // 删除 server
                    dataChangeListener.onRemove(beforeChild);
                }
            }
        }
    }

    public interface DataChangeListener {
        void onAdd(ServerRunningData serverRunningData);

        void onRemove(String key);
    }

    /**
     * 获取zk上注册的server地址
     *
     * @param zkServers zk地址
     * @param catalog   目录名
     * @return 实际server地址
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
     * @param zkServers zk地址
     * @param catalog   目录名
     * @return 所有服务地址
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
}
