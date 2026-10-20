package com.jdbcagent.client.nio;

import com.jdbcagent.client.JdbcAgentConnector;
import com.jdbcagent.client.uitl.ZookeeperUtil;
import com.jdbcagent.core.protocol.ClientAuth;
import com.jdbcagent.core.protocol.Packet;
import com.jdbcagent.core.util.TransportSecurity;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import static com.jdbcagent.core.protocol.Packet.PacketType;

/**
 * JDBC-Agent client tcp nio connector
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class JdbcAgentNioClient implements JdbcAgentConnector {
    private volatile boolean connected = false;         // 是否已经连接

    private SocketAddress address;                      // socket地址
    private SSLSocket socket;                           // TLS 连接
    private SSLContext sslContext;                      // 信任库
    private String username;                            // 账号
    private String password;                            // 密码
    private String zkServers;                           // zookeeper 地址
    private String catalog;                             // 目录名
    private AddressResolver addressResolver;            // 测试或自定义的地址解析

    private int soTimeout = 60000;                      // 超时时间
    private int idleTimeout = 30 * 60 * 1000;           // 连接超时时间

    private final Object readDataLock = new Object();   // 读的排他锁
    private final Object writeDataLock = new Object();  // 写的排他锁

    private final ByteBuffer readHeader = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
    private final ByteBuffer writeHeader = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);

    /**
     * 连接器构造方法
     *
     * @param address     地址
     * @param idleTimeout 超时时间
     */
    public JdbcAgentNioClient(SocketAddress address, int idleTimeout) {
        this.address = address;
        this.idleTimeout = idleTimeout;
    }

    public JdbcAgentNioClient(String username, String password, SocketAddress address, int idleTimeout) {
        this.username = username;
        this.password = password;
        this.address = address;
        this.idleTimeout = idleTimeout;
    }

    public JdbcAgentNioClient(String username, String password, String zkServers, String catalog, int idleTimeout)
            throws SQLException {
        this.username = username;
        this.password = password;
        this.zkServers = zkServers;
        this.catalog = catalog;
        this.idleTimeout = idleTimeout;
        this.address = ZookeeperUtil.toSocketAddress(ZookeeperUtil.getAddressFromZk(zkServers, catalog));
        ZookeeperUtil.watch(zkServers, catalog, new ZookeeperUtil.AddressListener() {
            @Override
            public void onAddress(String activeAddress) {
                onActiveAddress(activeAddress);
            }
        });
    }

    public void setSslContext(SSLContext sslContext) {
        this.sslContext = sslContext;
    }

    public SocketAddress getAddress() {
        return address;
    }

    void setAddressResolver(AddressResolver addressResolver) {
        this.addressResolver = addressResolver;
    }

    public SocketAddress getNextAddress() {
        if (addressResolver == null && (zkServers == null || catalog == null)) {
            return null;
        }
        try {
            String ipPort = addressResolver != null
                    ? addressResolver.resolve()
                    : ZookeeperUtil.getAddressFromZk(zkServers, catalog);
            if (ipPort == null || ipPort.length() == 0) {
                return null;
            }
            SocketAddress next = ZookeeperUtil.toSocketAddress(ipPort);
            address = next;
            return next;
        } catch (SQLException e) {
            return null;
        }
    }

    private void onActiveAddress(String ipPort) {
        SocketAddress next = ZookeeperUtil.toSocketAddress(ipPort);
        boolean reconnect = false;
        synchronized (writeDataLock) {
            if (next.equals(address)) {
                return;
            }
            address = next;
            if (connected) {
                doDisconnnect();
                connected = false;
                reconnect = true;
            }
        }
        if (reconnect) {
            connect();
        }
    }

    /**
     * 连接 server
     *
     * @throws SQLException
     */
    public void connect() {
        synchronized (writeDataLock) {
            if (connected) {
                return;
            }
            doConnect();
            connected = true;
        }
    }

    /**
     * 断开连接
     */
    public void disconnect() {
        connected = false;
        doDisconnnect();
    }


    private InetSocketAddress doConnect() {
        SocketAddress target = address != null ? address : getNextAddress();
        try {
            connectChannel(target);
        } catch (SQLException e) {
            doDisconnnect();
            throw new RuntimeException(e);
        } catch (Exception first) {
            doDisconnnect();
            if (zkServers == null && addressResolver == null) {
                throw new RuntimeException(first);
            }
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(ZookeeperUtil.FAILOVER_WAIT_MILLIS);
            Exception last = first;
            while (System.nanoTime() < deadline) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(last);
                }
                SocketAddress next = getNextAddress();
                if (next == null) {
                    continue;
                }
                try {
                    connectChannel(next);
                    last = null;
                    break;
                } catch (SQLException e) {
                    doDisconnnect();
                    throw new RuntimeException(e);
                } catch (Exception e) {
                    doDisconnnect();
                    last = e;
                }
            }
            if (last != null) {
                throw new RuntimeException(last);
            }
        }
        return new InetSocketAddress(socket.getLocalAddress(), socket.getLocalPort());
    }

    private void connectChannel(SocketAddress target) throws Exception {
        if (target == null) {
            throw new IOException("jdbc agent address is missing");
        }
        if (sslContext == null) {
            throw new IOException("sslTrustStore is required");
        }
        if (!(target instanceof InetSocketAddress)) {
            throw new IOException("jdbc agent address is missing");
        }
        InetSocketAddress inet = (InetSocketAddress) target;
        java.net.Socket tcp = new java.net.Socket();
        try {
            tcp.connect(inet, (int) Math.min(soTimeout, Integer.MAX_VALUE));
            tcp.setSoTimeout(soTimeout);
            SSLSocket created = (SSLSocket) sslContext.getSocketFactory()
                    .createSocket(tcp, inet.getHostString(), inet.getPort(), true);
            TransportSecurity.restrict(created);
            created.setSoTimeout(soTimeout);
            socket = created;
            created.startHandshake();
        } catch (Exception e) {
            if (socket == null) {
                try {
                    tcp.close();
                } catch (IOException ignored) {
                    // 握手失败时关掉尚未交给 SSLSocket 的连接
                }
            }
            throw e;
        }

        ClientAuth ca = ClientAuth.newBuilder()
                .setUsername(username != null ? username : "")
                .setPassword(password != null ? password : "")
                .setNetReadTimeout(idleTimeout)
                .setNetWriteTimeout(idleTimeout)
                .build();

        Packet.parse(write(Packet.newBuilder()
                .incrementAndGetId()
                .setType(PacketType.CLIENT_AUTH)
                .setBody(ca).build())).getAck();
        connected = true;
    }

    private void doDisconnnect() {
        if (socket != null) {
            quietlyClose(socket);
            socket = null;
        }
    }

    private void quietlyClose(Closeable channel) {
        try {
            channel.close();
        } catch (IOException e) {
            throw new RuntimeException("exception on closing channel:" + channel, e);
        }
    }

    /**
     * 写数据包
     *
     * @param packet 数据包
     * @return
     */
    public byte[] write(Packet packet) {
        synchronized (writeDataLock) {
            try {
                byte[] body = packet.toByteArray();
                Packet.checkBodyLength(body.length);
                writeHeader.clear();
                writeHeader.putInt(body.length);
                writeHeader.flip();
                byte[] header = new byte[4];
                writeHeader.get(header);
                OutputStream out = socket.getOutputStream();
                out.write(header);
                out.write(body);
                out.flush();

                // 写完立即从通道读取返回值
                return read();
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage());
            }
        }
    }

    /**
     * 读取数据
     *
     * @return 数据包二进制数据
     */
    private byte[] read() {
        synchronized (readDataLock) {
            try {
                readHeader.clear();
                readFully(readHeader);
                int bodyLen = readHeader.getInt(0);
                Packet.checkBodyLength(bodyLen);
                if (bodyLen != 0) {
                    ByteBuffer bodyBuf = ByteBuffer.allocate(bodyLen).order(ByteOrder.BIG_ENDIAN);
                    readFully(bodyBuf);
                    return bodyBuf.array();
                } else {
                    return null;
                }
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage());
            }
        }
    }

    private void readFully(ByteBuffer buffer) throws IOException {
        InputStream in = socket.getInputStream();
        byte[] raw = new byte[buffer.remaining()];
        int offset = 0;
        try {
            while (offset < raw.length) {
                int read = in.read(raw, offset, raw.length - offset);
                if (read == -1) {
                    throw new IOException("end of stream when reading header");
                }
                offset += read;
            }
        } catch (SocketTimeoutException timeout) {
            throw new SocketTimeoutException("timeout waiting for response");
        }
        buffer.put(raw);
    }

    public void start() {

    }

    public void stop() {

    }

    interface AddressResolver {
        String resolve() throws SQLException;
    }
}
