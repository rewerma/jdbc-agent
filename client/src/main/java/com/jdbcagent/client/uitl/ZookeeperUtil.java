package com.jdbcagent.client.uitl;

import com.jdbcagent.core.util.ByteSerializer;
import com.jdbcagent.core.util.ZookeeperPathUtils;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;

import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * JDBC-Agent client 获取zk工具类
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class ZookeeperUtil {
    public static final long FAILOVER_WAIT_MILLIS = 8000L;

    private static final Map<String, ZkClient> CLIENTS = new ConcurrentHashMap<String, ZkClient>();
    private static final Map<String, AddressHub> HUBS = new ConcurrentHashMap<String, AddressHub>();

    /**
     * 当前 active 地址变化时回调。address 为 host:port。
     */
    public interface AddressListener {
        void onAddress(String address);
    }

    /**
     * 获取zk上注册的server地址
     *
     * @param zkServers zk地址
     * @param catalog   目录名
     * @return 实际server地址
     * @throws SQLException
     */
    public static String getAddressFromZk(String zkServers, String catalog) throws SQLException {
        try {
            byte[] bytes = client(zkServers).readData(runningPath(catalog), true);
            if (bytes == null) {
                throw new SQLException("Error to connect jdbc agent server");
            }
            return readAddress(new String(bytes, "UTF-8"));
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    /**
     * 在超时时间内反复读取 active 地址，覆盖备机抢占前的空窗。
     */
    public static String awaitAddress(String zkServers, String catalog, long timeoutMs) throws SQLException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        SQLException last = null;
        while (true) {
            try {
                return getAddressFromZk(zkServers, catalog);
            } catch (SQLException e) {
                last = e;
            }
            if (System.nanoTime() >= deadline) {
                throw last != null ? last : new SQLException("active jdbc agent server is not available");
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SQLException("interrupted while waiting for active server", e);
            }
        }
    }

    /**
     * 监听 running 节点。地址变化后通知，已有地址会立刻回调一次。
     */
    public static void watch(String zkServers, String catalog, final AddressListener listener) throws SQLException {
        final String key = watchKey(zkServers, catalog);
        final AddressHub hub;
        final boolean created;
        synchronized (HUBS) {
            AddressHub existing = HUBS.get(key);
            created = existing == null;
            hub = created ? new AddressHub() : existing;
            if (created) {
                HUBS.put(key, hub);
            }
            hub.add(listener);
        }
        if (created) {
            try {
                subscribe(zkServers, catalog, hub);
            } catch (SQLException e) {
                synchronized (HUBS) {
                    HUBS.remove(key, hub);
                }
                throw e;
            } catch (Exception e) {
                synchronized (HUBS) {
                    HUBS.remove(key, hub);
                }
                throw new SQLException(e);
            }
        } else {
            String current = hub.current();
            if (current != null) {
                listener.onAddress(current);
            }
        }
    }

    public static String readAddress(String json) throws SQLException {
        if (json == null) {
            throw new SQLException("active server address is missing");
        }
        int from = 0;
        while (from < json.length()) {
            int keyAt = json.indexOf("\"address\"", from);
            if (keyAt < 0) {
                break;
            }
            int index = keyAt + "\"address\"".length();
            while (index < json.length() && Character.isWhitespace(json.charAt(index))) {
                index++;
            }
            if (index < json.length() && json.charAt(index) == ':') {
                index++;
                while (index < json.length() && Character.isWhitespace(json.charAt(index))) {
                    index++;
                }
                if (index < json.length() && json.charAt(index) == '"') {
                    return readJsonString(json, index + 1);
                }
            }
            from = keyAt + "\"address\"".length();
        }
        throw new SQLException("active server address is missing");
    }

    public static InetSocketAddress toSocketAddress(String ipPort) {
        int colon = ipPort.lastIndexOf(':');
        if (colon <= 0 || colon == ipPort.length() - 1) {
            throw new IllegalArgumentException("illegal address: " + ipPort);
        }
        String host = ipPort.substring(0, colon);
        int port = Integer.parseInt(ipPort.substring(colon + 1));
        return new InetSocketAddress(host, port);
    }

    private static String readJsonString(String json, int start) throws SQLException {
        StringBuilder value = new StringBuilder();
        for (int index = start; index < json.length(); index++) {
            char current = json.charAt(index);
            if (current == '\\') {
                if (index + 1 >= json.length()) {
                    throw new SQLException("active server address is missing");
                }
                value.append(json.charAt(index + 1));
                index++;
            } else if (current == '"') {
                if (value.length() == 0) {
                    throw new SQLException("active server address is missing");
                }
                return value.toString();
            } else {
                value.append(current);
            }
        }
        throw new SQLException("active server address is missing");
    }

    private static void subscribe(final String zkServers, final String catalog, final AddressHub hub) throws Exception {
        final ZkClient zkClient = client(zkServers);
        zkClient.subscribeDataChanges(runningPath(catalog), new IZkDataListener() {
            @Override
            public void handleDataChange(String dataPath, Object data) throws Exception {
                if (data instanceof byte[]) {
                    hub.publish(readAddress(new String((byte[]) data, "UTF-8")));
                }
            }

            @Override
            public void handleDataDeleted(String dataPath) {
                hub.clear();
            }
        });
        byte[] bytes = zkClient.readData(runningPath(catalog), true);
        if (bytes != null) {
            hub.publishInitial(readAddress(new String(bytes, "UTF-8")));
        }
    }

    private static ZkClient client(String zkServers) {
        ZkClient existing = CLIENTS.get(zkServers);
        if (existing != null) {
            return existing;
        }
        ZkClient created = new ZkClient(zkServers, 3000, 3000, new ByteSerializer());
        ZkClient raced = CLIENTS.putIfAbsent(zkServers, created);
        if (raced != null) {
            created.close();
            return raced;
        }
        return created;
    }

    private static String runningPath(String catalog) {
        return ZookeeperPathUtils.JA_ROOT_NODE + ZookeeperPathUtils.ZOOKEEPER_SEPARATOR
                + catalog + ZookeeperPathUtils.ZOOKEEPER_SEPARATOR
                + ZookeeperPathUtils.RUNNING_NODE;
    }

    private static String watchKey(String zkServers, String catalog) {
        return zkServers + "\0" + catalog;
    }

    static final class AddressHub {
        private final CopyOnWriteArrayList<AddressListener> listeners = new CopyOnWriteArrayList<AddressListener>();
        private String address;
        private boolean seen;

        void add(AddressListener listener) {
            listeners.add(listener);
        }

        synchronized String current() {
            return address;
        }

        void publish(String candidate) {
            if (candidate == null || candidate.length() == 0) {
                return;
            }
            boolean changed;
            synchronized (this) {
                seen = true;
                changed = !candidate.equals(address);
                address = candidate;
            }
            if (changed) {
                notifyListeners(candidate);
            }
        }

        void publishInitial(String candidate) {
            if (candidate == null || candidate.length() == 0) {
                return;
            }
            boolean changed;
            synchronized (this) {
                if (seen) {
                    return;
                }
                seen = true;
                changed = !candidate.equals(address);
                address = candidate;
            }
            if (changed) {
                notifyListeners(candidate);
            }
        }

        synchronized void clear() {
            seen = true;
            address = null;
        }

        private void notifyListeners(String candidate) {
            for (AddressListener listener : listeners) {
                try {
                    listener.onAddress(candidate);
                } catch (RuntimeException ignored) {
                    // 单个客户端切换失败不影响其他监听者
                }
            }
        }
    }
}
