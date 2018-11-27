package com.jdbcagent.server.running;

import com.alibaba.fastjson.JSON;
import com.jdbcagent.core.util.ServerRunningData;
import com.jdbcagent.core.util.ZookeeperPathUtils;
import com.jdbcagent.server.config.JdbcAgentConf;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * JDBC-Agent server 运行监控器
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class ServerRunningMonitor {
    private static final Logger logger = LoggerFactory.getLogger(ServerRunningMonitor.class);

    private volatile boolean running = false; // 是否处于运行中
    private ZkClient zkClient;
    private JdbcAgentConf.Catalog catalog;                  // 目录名
    private ServerRunningData serverData;                   // server数据
    private volatile boolean release = false;
    private volatile ServerRunningData activeData;
    private ScheduledExecutorService delayExecutor = Executors.newScheduledThreadPool(1);
    private volatile boolean manual = false;                // 手动操作zk

    private String getServerRunning(String catalog) {
        return MessageFormat.format(ZookeeperPathUtils.CATALOG_NODE + ZookeeperPathUtils.ZOOKEEPER_SEPARATOR
                + serverData.getAddress(), catalog);
    }

    private static String getServerNodePath(String catalog) {
        return MessageFormat.format(ZookeeperPathUtils.CATALOG_NODE, catalog);
    }

    public boolean isStart() {
        return running;
    }

    public void start() {
        if (running) {
            throw new RuntimeException(this.getClass().getName() + " has startup , don't repeat start");
        }
        running = true;

        initRunning();
    }

    public void stop() {
        if (!running) {
            throw new RuntimeException(this.getClass().getName() + " isn't start , please check");
        }
        running = false;

        String path = getServerRunning(this.catalog.getCatalog());
        releaseRunning(); // 尝试release
        if (delayExecutor != null) {
            delayExecutor.shutdown();
        }
    }

    public synchronized void initRunning() {
        if (!isStart()) {
            return;
        }

        String path = getServerRunning(this.catalog.getCatalog());
        // 序列化
        byte[] bytes = JSON.toJSONBytes(serverData);
        try {
            zkClient.create(path, bytes, CreateMode.EPHEMERAL);
            activeData = serverData;
        } catch (ZkNodeExistsException e) {
            bytes = zkClient.readData(path, true);
            if (bytes == null) {// 如果不存在节点，立即尝试一次
                initRunning();
            } else {
                activeData = JSON.parseObject(bytes, ServerRunningData.class);
            }
        } catch (ZkNoNodeException e) {
            zkClient.createPersistent(getServerNodePath(this.catalog.getCatalog()),
                    true); // 尝试创建父节点
            initRunning();
        } catch (Throwable t) {
            logger.error(MessageFormat.format("There is an error when execute initRunning method, with destination [{0}].",
                    catalog),
                    t);
            // 出现任何异常尝试release
            releaseRunning();
            throw new RuntimeException("something goes wrong in initRunning method. ", t);
        }
    }


    /**
     * 检查当前的状态
     */
    public boolean check() {
        String path = getServerRunning(this.catalog.getCatalog());
        //ZookeeperPathUtils.getDestinationClientRunning(this.destination, clientData.getClientId());
        try {
            byte[] bytes = zkClient.readData(path);
            ServerRunningData eventData = JSON.parseObject(bytes, ServerRunningData.class);
            activeData = eventData;// 更新下为最新值
            // 检查下nid是否为自己
            boolean result = isMine(activeData.getAddress());
            if (!result) {
                logger.warn("jdbc-agent server is running in [{}] , but not in [{}]",
                        activeData.getAddress(),
                        serverData.getAddress());
            }
            return result;
        } catch (ZkNoNodeException e) {
            logger.warn("jdbc-agent server is not run any in node");
            return false;
        } catch (ZkInterruptedException e) {
            logger.warn("jdbc-agent server check is interrupt");
            Thread.interrupted();// 清除interrupt标记
            return check();
        } catch (ZkException e) {
            logger.warn("jdbc-agent server check is failed");
            return false;
        }
    }

    public boolean releaseRunning() {
        if (check()) {
            String path = getServerRunning(this.catalog.getCatalog());
            zkClient.delete(path);
            return true;
        }

        return false;
    }


    // ====================== helper method ======================

    private boolean isMine(String address) {
        return address.equals(serverData.getAddress());
    }

    // ===================== setter / getter =======================

    public JdbcAgentConf.Catalog getCatalog() {
        return catalog;
    }

    public void setCatalog(JdbcAgentConf.Catalog catalog) {
        this.catalog = catalog;
    }

    public void setServerData(ServerRunningData serverData) {
        this.serverData = serverData;
    }

    public void setZkClient(ZkClient zkClient) {
        this.zkClient = zkClient;
    }
}
