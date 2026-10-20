package com.jdbcagent.client.netty;

import com.jdbcagent.core.protocol.Packet;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * JDBC-Agent server netty 工具类
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class NettyUtils {
    public static final ReentrantLock lock = new ReentrantLock();

    public static final Condition CONNECTED = lock.newCondition();

    public static ConcurrentHashMap<Long, NettyResponse> RESPONSE_MAP = new ConcurrentHashMap<>();

    private final static int HEADER_LENGTH = 4;   // 数据包头长度

    /**
     * 向客户端写数据
     *
     * @param channel              通道
     * @param packet               数据包
     * @param channelFutureListner
     */
    public static void write(Channel channel, Packet packet, ChannelFutureListener channelFutureListner) {
        byte[] body = packet.toByteArray();
        Packet.checkBodyLength(body.length);
        byte[] header = ByteBuffer.allocate(HEADER_LENGTH).order(ByteOrder.BIG_ENDIAN)
                .putInt(body.length).array();
        ChannelFuture future = channel.writeAndFlush(Unpooled.wrappedBuffer(header, body));
        if (channelFutureListner != null) {
            future.addListener(channelFutureListner);
        }
    }

    /**
     * 等待响应。调用方必须已持有 {@link #lock}。超时或连接失败时抛出 SQLException，不会一直挂起。
     */
    public static Packet awaitResponse(NettyResponse response, long timeoutMs) throws SQLException, InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (response.getPacket() == null && response.getError() == null) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0 || !response.getCondition().await(remaining, TimeUnit.NANOSECONDS)) {
                if (response.getPacket() == null && response.getError() == null) {
                    throw new SQLException("timeout waiting for response");
                }
            }
        }
        if (response.getError() != null) {
            throw new SQLException(response.getError());
        }
        return response.getPacket();
    }

    /**
     * 连接断开时唤醒仍在等待的请求。
     */
    public static void failPending(String message) {
        lock.lock();
        try {
            if (!JdbcAgentNettyClient.connected && JdbcAgentNettyClient.authError == null) {
                JdbcAgentNettyClient.authError = message;
            }
            CONNECTED.signalAll();
            for (NettyResponse response : RESPONSE_MAP.values()) {
                if (response.getPacket() == null && response.getError() == null) {
                    response.setError(message);
                }
                if (response.getCondition() != null) {
                    response.getCondition().signalAll();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public static class NettyResponse {
        private Condition condition;
        private Packet packet;
        private String error;

        public Condition getCondition() {
            return condition;
        }

        public void setCondition(Condition condition) {
            this.condition = condition;
        }

        public Packet getPacket() {
            return packet;
        }

        public void setPacket(Packet packet) {
            this.packet = packet;
        }

        public String getError() {
            return error;
        }

        public void setError(String error) {
            this.error = error;
        }
    }
}
