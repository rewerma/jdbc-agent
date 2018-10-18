package com.jdbcagent.client.netty;

import com.jdbcagent.client.uitl.SerializeUtil;
import com.jdbcagent.core.protocol.Packet;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.Channels;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.CountDownLatch;

/**
 * JDBC-Agent server netty 工具类
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class NettyUtils {
    private final static int HEADER_LENGTH = 4;   // 数据包头长度

    /**
     * 向客户端写数据
     *
     * @param channel               通道
     * @param packet                数据包
     * @param channelFutureListener
     */
    public static void write(Channel channel, Packet packet, ChannelFutureListener channelFutureListener) {
        write(channel, packet.toByteArray(SerializeUtil.serializeType), channelFutureListener);
    }

    /**
     * 向客户端写数据
     *
     * @param channel               通道
     * @param body                  数据体
     * @param channelFutureListener
     */
    public static void write(Channel channel, byte[] body, ChannelFutureListener channelFutureListener) {
        byte[] header = ByteBuffer.allocate(HEADER_LENGTH).order(ByteOrder.BIG_ENDIAN)
                .putInt(body.length).array();
        if (channelFutureListener == null) {
            Channels.write(channel, ChannelBuffers.wrappedBuffer(header, body));
        } else {
            Channels.write(channel, ChannelBuffers.wrappedBuffer(header, body))
                    .addListener(channelFutureListener);
        }
    }

    public static class NettyResponse {
        private Packet packet;
        private CountDownLatch latch;

        public Packet getPacket() {
            return packet;
        }

        public void setPacket(Packet packet) {
            this.packet = packet;
        }

        public CountDownLatch getLatch() {
            return latch;
        }

        public void setLatch(CountDownLatch latch) {
            this.latch = latch;
        }
    }
}
