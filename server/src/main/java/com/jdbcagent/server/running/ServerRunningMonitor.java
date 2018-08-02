package com.jdbcagent.server.running;

import com.alibaba.fastjson.JSON;
import com.jdbcagent.core.util.ZookeeperPathUtils;
import com.jdbcagent.server.config.JdbcAgentConf;
import com.jdbcagent.server.util.AddressUtils;
import com.jdbcagent.server.util.BooleanMutex;
import org.I0Itec.zkclient.IZkDataListener;
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
import java.util.concurrent.TimeUnit;

/**
 * JDBC-Agent server 运行监控器
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class ServerRunningMonitor {
    private static final Logger logger = LoggerFactory.getLogger(ServerRunningMonitor.class);

    private volatile boolean running = false; // 是否处于运行中
    private JdbcAgentConf jdbcAgentConf;                    // 配置项
    private ZkClient zkClient;
    private JdbcAgentConf.Catalog catalog;                  // 目录名
    private ServerRunningData serverData;                   // server数据
    private IZkDataListener dataListener;
    private BooleanMutex mutex = new BooleanMutex(false);
    private volatile boolean release = false;
    private volatile ServerRunningData activeData;
    private ScheduledExecutorService delayExecutor = Executors.newScheduledThreadPool(1);
    private ServerRunningListener listener;
    private volatile boolean manual = false;                // 手动操作zk

    private static String getServerRunning(String catalog) {
        return MessageFormat.format(ZookeeperPathUtils.CATALOG_NODE + ZookeeperPathUtils.ZOOKEEPER_SEPARATOR
                + ZookeeperPathUtils.RUNNING_NODE, catalog);
    }

    private static String getServerNodePath(String catalog) {
        return MessageFormat.format(ZookeeperPathUtils.CATALOG_NODE, catalog);
    }

    public ServerRunningMonitor() {
        dataListener = new IZkDataListener() {
            public void handleDataChange(String dataPath, Object data) throws Exception {
                ServerRunningData runningData = JSON.parseObject((byte[]) data, ServerRunningData.class);
                if (!isMine(runningData.getAddress())) {
                    mutex.set(false);
                }

                // 说明出现了主动释放的操作，并且本机之前是active
                if (!runningData.isActive() && isMine(runningData.getAddress())) {
                    release = true;
                    releaseRunning();// 彻底释放mainstem
                    manual = true;
                }

                activeData = runningData;
            }

            public void handleDataDeleted(String dataPath) throws Exception {
                mutex.set(false);
                // 触发一下退出,可能是人为干预的释放操作或者网络闪断引起的session expired timeout
                processActiveExit();
                if (!release && activeData != null && isMine(activeData.getAddress())) {
                    // 如果上一次active的状态就是本机，则即时触发一下active抢占
                    initRunning();
                } else {
                    // 否则就是等待delayTime，避免因网络瞬端或者zk异常，导致出现频繁的切换操作
                    int delayTime;
                    if (manual) {
                        // 如果是手动操作，本机延迟启动
                        delayTime = 10;
                        manual = false;
                    } else {
                        delayTime = 3;
                    }
                    delayExecutor.schedule(new Runnable() {

                        public void run() {
                            initRunning();
                        }
                    }, delayTime, TimeUnit.SECONDS);
                }
            }
        };
    }

    public boolean isStart() {
        return running;
    }

    public void start() {
        if (running) {
            throw new RuntimeException(this.getClass().getName() + " has startup , don't repeat start");
        }
        running = true;

        String path = getServerRunning(this.catalog.getCatalog());

        zkClient.subscribeDataChanges(path, dataListener);
        initRunning();
    }

    public void stop() {
        if (!running) {
            throw new RuntimeException(this.getClass().getName() + " isn't start , please check");
        }
        running = false;

        String path = getServerRunning(this.catalog.getCatalog());
        zkClient.unsubscribeDataChanges(path, dataListener);
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
            mutex.set(false);
            zkClient.create(path, bytes, CreateMode.EPHEMERAL);
            processActiveEnter();// 触发一下事件
            activeData = serverData;
            mutex.set(true);
        } catch (ZkNodeExistsException e) {
            bytes = zkClient.readData(path, true);
            if (bytes == null) {// 如果不存在节点，立即尝试一次
                initRunning();
            } else {
                activeData = JSON.parseObject(bytes, ServerRunningData.class);
                // 如果发现已经存在,判断一下是否自己,避免活锁
                if (activeData.getAddress().contains(":") && isMine(activeData.getAddress())) {
                    mutex.set(true);
                }
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

    /**
     * 阻塞等待自己成为active，如果自己成为active，立马返回
     *
     * @throws InterruptedException
     */
    public void waitForActive() throws InterruptedException {
        initRunning();
        mutex.get();
    }

    public boolean releaseRunning() {
        if (check()) {
            String path = getServerRunning(this.catalog.getCatalog());
            zkClient.delete(path);
            mutex.set(false);
            processActiveExit();
            return true;
        }

        return false;
    }

    public void setListener(ServerRunningListener listener) {
        this.listener = listener;
    }

    // ====================== helper method ======================

    private boolean isMine(String address) {
        return address.equals(serverData.getAddress());
    }

    private void processActiveEnter() {
        if (listener != null) {
            // 触发回调
            listener.processActiveEnter();
            String ip = jdbcAgentConf.getJdbcAgent().getIp();
            if (ip == null) {
                ip = AddressUtils.getHostIp();
            }
            this.serverData.setAddress(ip + ":" + jdbcAgentConf.getJdbcAgent().getPort());

            String path = getServerRunning(this.catalog.getCatalog());
            // 序列化
            byte[] bytes = JSON.toJSONBytes(serverData);
            zkClient.writeData(path, bytes);
        }
    }

    private void processActiveExit() {
        if (listener != null) {
            listener.processActiveExit();
        }
    }

    // ===================== setter / getter =======================

    public void setJdbcAgentConf(JdbcAgentConf jdbcAgentConf) {
        this.jdbcAgentConf = jdbcAgentConf;
    }

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
