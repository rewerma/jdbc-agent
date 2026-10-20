package com.jdbcagent.client.uitl;

import com.jdbcagent.core.util.ByteSerializer;
import com.jdbcagent.core.util.ClusterNode;
import com.jdbcagent.core.util.ZookeeperPathUtils;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;

import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    private static final Map<String, Membership> MEMBERSHIPS = new ConcurrentHashMap<String, Membership>();

    /**
     * 当前 active 地址变化时回调。address 为 host:port。
     */
    public interface AddressListener {
        void onAddress(String address);
    }

    /**
     * 集群有存活成员时按负载选择，否则回到主备 running 节点。
     */
    public static Selection select(String zkServers, String catalog, Set<String> excluded) throws SQLException {
        List<ClusterNode> nodes = members(zkServers, catalog);
        if (!nodes.isEmpty()) {
            ClusterNode node = ClusterPicker.pick(nodes, excluded);
            if (node == null) {
                node = ClusterPicker.pick(nodes, Collections.<String>emptySet());
            }
            if (node == null) {
                throw new SQLException("jdbc agent cluster has no server");
            }
            return new Selection(true, node.getAddress());
        }
        return new Selection(false, getAddressFromZk(zkServers, catalog));
    }

    /**
     * 读取并监听 /servers 下的存活实例。父节点不存在时返回空列表。
     */
    public static List<ClusterNode> members(String zkServers, String catalog) throws SQLException {
        String key = watchKey(zkServers, catalog);
        Membership membership = membership(zkServers, catalog, key);
        if (membership.loaded) {
            return membership.nodes;
        }
        return readMembers(zkServers, catalog, membership);
    }

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

    private static Membership membership(final String zkServers, final String catalog, String key) throws SQLException {
        Membership existing = MEMBERSHIPS.get(key);
        if (existing != null) {
            return existing;
        }
        final Membership created = new Membership();
        try {
            final ZkClient zkClient = client(zkServers);
            zkClient.subscribeChildChanges(ZookeeperPathUtils.serversPath(catalog), new IZkChildListener() {
                @Override
                public void handleChildChange(String parentPath, List<String> currentChilds) {
                    try {
                        if (currentChilds == null) {
                            readMembers(zkServers, catalog, created);
                        } else {
                            created.publish(loadChildren(zkClient, parentPath, currentChilds));
                        }
                    } catch (Exception ignored) {
                        // 保留上一份成员列表，下一次建连会再读
                    }
                }
            });
        } catch (ZkNoNodeException e) {
            return created;
        } catch (Exception e) {
            throw new SQLException(e);
        }
        Membership raced = MEMBERSHIPS.putIfAbsent(key, created);
        return raced == null ? created : raced;
    }

    private static List<ClusterNode> readMembers(String zkServers, String catalog, Membership membership)
            throws SQLException {
        try {
            ZkClient zkClient = client(zkServers);
            String path = ZookeeperPathUtils.serversPath(catalog);
            List<String> children = zkClient.getChildren(path);
            List<ClusterNode> nodes = loadChildren(zkClient, path, children);
            membership.publish(nodes);
            return nodes;
        } catch (ZkNoNodeException e) {
            return Collections.emptyList();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    private static List<ClusterNode> loadChildren(ZkClient zkClient, String parent, List<String> children) {
        if (children == null || children.isEmpty()) {
            return Collections.emptyList();
        }
        List<ClusterNode> nodes = new ArrayList<ClusterNode>(children.size());
        for (int i = 0; i < children.size(); i++) {
            String child = children.get(i);
            try {
                byte[] bytes = zkClient.readData(parent + ZookeeperPathUtils.ZOOKEEPER_SEPARATOR + child, true);
                ClusterNode node = ClusterNode.parse(bytes);
                if (node != null) {
                    nodes.add(node);
                }
            } catch (ZkNoNodeException ignored) {
                // 列举之后节点已消失
            }
        }
        return nodes;
    }

    static final class Membership {
        private volatile List<ClusterNode> nodes = Collections.emptyList();
        private volatile boolean loaded;

        void publish(List<ClusterNode> current) {
            nodes = current == null ? Collections.<ClusterNode>emptyList() : Collections.unmodifiableList(current);
            loaded = true;
        }
    }

    public static final class Selection {
        private final boolean cluster;
        private final String address;

        Selection(boolean cluster, String address) {
            this.cluster = cluster;
            this.address = address;
        }

        public boolean isCluster() {
            return cluster;
        }

        public String getAddress() {
            return address;
        }
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
