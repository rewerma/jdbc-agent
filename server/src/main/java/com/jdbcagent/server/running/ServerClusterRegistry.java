package com.jdbcagent.server.running;

import com.jdbcagent.core.util.ClusterNode;
import com.jdbcagent.core.util.ZookeeperPathUtils;
import com.jdbcagent.server.config.JdbcAgentConf;
import com.jdbcagent.server.jdbc.ConnectionServer;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.zookeeper.CreateMode;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 把本实例注册成集群成员。会话断开后临时节点消失，客户端不再把新连接分过来。
 */
public class ServerClusterRegistry {
    static final long PUBLISH_INTERVAL_SECONDS = 5L;

    private final JdbcAgentConf jdbcAgentConf;
    private final ZkClient zkClient;
    private final ClusterNode node;
    private final String path;
    private final ScheduledExecutorService scheduler;
    private final AtomicInteger publishedConnections = new AtomicInteger(-1);
    private volatile boolean closed;

    public ServerClusterRegistry(JdbcAgentConf jdbcAgentConf, ZkClient zkClient, String address) {
        this.jdbcAgentConf = jdbcAgentConf;
        this.zkClient = zkClient;
        this.node = new ClusterNode();
        node.setAddress(address);
        node.setWeight(jdbcAgentConf.getJdbcAgent().getWeight());
        node.setActiveConnections(0);
        String catalog = jdbcAgentConf.getJdbcAgent().getCatalog();
        this.path = ZookeeperPathUtils.serversPath(catalog) + ZookeeperPathUtils.ZOOKEEPER_SEPARATOR
                + serverId(address);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable runnable) {
                Thread thread = new Thread(runnable, "jdbc-agent-cluster");
                thread.setDaemon(true);
                return thread;
            }
        });
    }

    public void register() {
        String catalog = jdbcAgentConf.getJdbcAgent().getCatalog();
        String catalogPath = ZookeeperPathUtils.JA_ROOT_NODE + ZookeeperPathUtils.ZOOKEEPER_SEPARATOR + catalog;
        zkClient.createPersistent(catalogPath, true);
        zkClient.createPersistent(ZookeeperPathUtils.serversPath(catalog), true);
        publish(ConnectionServer.connectionCount());
        zkClient.create(path, ClusterNode.toBytes(node), CreateMode.EPHEMERAL);
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                refresh();
            }
        }, PUBLISH_INTERVAL_SECONDS, PUBLISH_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        scheduler.shutdownNow();
        try {
            zkClient.delete(path);
        } catch (ZkNoNodeException ignored) {
            // 会话已经丢掉这个临时节点
        } finally {
            try {
                zkClient.close();
            } catch (RuntimeException ignored) {
                // 关闭客户端失败不阻止端口退出
            }
        }
    }

    static String serverId(String address) {
        return address.replace(':', '_').replace('/', '_');
    }

    private void refresh() {
        if (closed) {
            return;
        }
        int count = ConnectionServer.connectionCount();
        if (publishedConnections.get() == count) {
            return;
        }
        publish(count);
        try {
            zkClient.writeData(path, ClusterNode.toBytes(node));
        } catch (RuntimeException ignored) {
            // 下一次周期再写。节点丢失说明会话已经结束
        }
    }

    private void publish(int count) {
        node.setActiveConnections(count);
        publishedConnections.set(count);
    }
}
