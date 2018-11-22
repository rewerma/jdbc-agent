package com.jdbcagent.client.nio;

import com.jdbcagent.client.JdbcAgentConnector;
import com.jdbcagent.client.uitl.SerializeUtil;
import com.jdbcagent.core.protocol.ClientAuth;
import com.jdbcagent.core.protocol.Packet;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * JDBC-Agent client tcp nio connector
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class JdbcAgentNioClient extends JdbcAgentConnector {
    private volatile boolean connected = false;         // 是否已经连接

    private SocketAddress address;                      // socket地址
    private SocketChannel channel;                      // socket通道
    private String username;                            // 账号
    private String password;                            // 密码

    private ReadableByteChannel readableChannel;        // 读通道

    private WritableByteChannel writableChannel;        // 写通道

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

    public SocketAddress getAddress() {
        return address;
    }

    public SocketAddress getNextAddress() {
        return null;
    }

    /**
     * 连接 server
     *
     * @throws SQLException
     */
    public void connect() {
        if (connected) {
            return;
        }
        doConnect();
        connected = true;
    }

    /**
     * 断开连接
     */
    public void disconnect() {
        connected = false;
        doDisconnect();
    }


    private InetSocketAddress doConnect() {
        try {
            channel = SocketChannel.open();
            channel.socket().setSoTimeout(soTimeout);
            SocketAddress address = getAddress();
            if (address == null) {
                address = getNextAddress();
            }
            channel.connect(address);
            readableChannel = Channels.newChannel(channel.socket().getInputStream());
            writableChannel = Channels.newChannel(channel.socket().getOutputStream());

            String packet = "93" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()) +
                    "|AA" + (username != null ? username : "") +
                    "|AD" + (password != null ? password : "") +
                    "|XW" + idleTimeout +
                    "|XR" + idleTimeout +
                    "|XV1";

            byte[] ackBody = write(packet.getBytes(StandardCharsets.UTF_8));
            String ackPacket = new String(ackBody, StandardCharsets.UTF_8);
            if (!ackPacket.startsWith("64")) {
                throw new RuntimeException("error ack from jdbc agent server ");
            }
            String[] packetItems = ackPacket.split("\\|");
            for (String packetItem : packetItems) {
                if (packetItem.startsWith("AS")) {
                    SerializeUtil.serializeType = Packet.SerializeType.valueOf(packetItem.substring(2));
                }
            }

            connected = true;
            return new InetSocketAddress(channel.socket().getLocalAddress(),
                    channel.socket().getLocalPort());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void doDisconnect() {
        if (readableChannel != null) {
            quietlyClose(readableChannel);
            readableChannel = null;
        }
        if (writableChannel != null) {
            quietlyClose(writableChannel);
            writableChannel = null;
        }
        if (channel != null) {
            quietlyClose(channel);
            channel = null;
        }
    }

    private void quietlyClose(Channel channel) {
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
        return write(packet.toByteArray(SerializeUtil.serializeType));
    }

    /**
     * 写数据包
     *
     * @param body 数据体
     * @return
     */
    public byte[] write(byte[] body) {
        synchronized (writeDataLock) {
            try {
                writeHeader.clear();
                writeHeader.putInt(body.length);
                writeHeader.flip();
                channel.write(writeHeader);
                channel.write(ByteBuffer.wrap(body));

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
                read(channel, readHeader);
                int bodyLen = readHeader.getInt(0);
                if (bodyLen != 0) {
                    ByteBuffer bodyBuf = ByteBuffer.allocate(bodyLen).order(ByteOrder.BIG_ENDIAN);
                    read(channel, bodyBuf);
                    return bodyBuf.array();
                } else {
                    return null;
                }
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage());
            }
        }
    }

    private void read(ReadableByteChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int r = channel.read(buffer);
            if (r == -1) {
                throw new IOException("end of stream when reading header");
            }
        }
    }
}
