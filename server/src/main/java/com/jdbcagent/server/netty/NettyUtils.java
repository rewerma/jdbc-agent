package com.jdbcagent.server.netty;

import com.jdbcagent.core.protocol.Ack;
import com.jdbcagent.core.protocol.Packet;
import com.jdbcagent.core.protocol.Packet.PacketType;
import com.jdbcagent.server.config.Configuration;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * JDBC-Agent server netty 工具类
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class NettyUtils {
    private final static int HEADER_LENGTH = 4;   // 数据包头长度

    public static Timer hashedWheelTimer = new HashedWheelTimer();

    /**
     * 向客户端写数据
     *
     * @param channel              通道
     * @param packet               数据包
     * @param channelFutureListner
     */
    public static void write(Channel channel, Packet packet, ChannelFutureListener channelFutureListner) {
        write(channel,
                packet.toByteArray(Configuration.getJdbcAgentCon().getJdbcAgent().getSerializeType()), channelFutureListner);
    }

    public static void write(Channel channel, byte[] body, ChannelFutureListener channelFutureListner) {
        byte[] header = ByteBuffer.allocate(HEADER_LENGTH).order(ByteOrder.BIG_ENDIAN)
                .putInt(body.length).array();
        if (channelFutureListner == null) {
            Channels.write(channel, ChannelBuffers.wrappedBuffer(header, body));
        } else {
            Channels.write(channel, ChannelBuffers.wrappedBuffer(header, body))
                    .addListener(channelFutureListner);
        }
    }

    /**
     * 向客户端应答数据
     *
     * @param channel
     * @param channelFutureListner
     */
    public static void ack(Channel channel, Packet packet, ChannelFutureListener channelFutureListner) {
        Packet packetAck =
                Packet.newBuilder(packet.getId())
                        .setType(PacketType.ACK)
                        .setBody(new Ack()).build();
        write(channel, packetAck, channelFutureListner);
    }

    /**
     * 认证响应客户端应答数据
     *
     * @param channel
     * @param channelFutureListner
     */
    public static void ackAuth(Channel channel, Packet packet, ChannelFutureListener channelFutureListner) {
        Packet packetAck =
                Packet.newBuilder(packet.getId())
                        .setType(PacketType.CLIENT_AUTH)
                        .setBody(new Ack()).build();
        write(channel, packetAck, channelFutureListner);
    }

    public static void ackAuth(Channel channel, byte[] body, ChannelFutureListener channelFutureListner) {
        write(channel, body, channelFutureListner);
    }

    /**
     * 响应错误信息
     *
     * @param errorCode             错误代码
     * @param errorMessage          错误信息
     * @param channel
     * @param channelFutureListener
     */
    public static void error(Packet packet, int errorCode, String errorMessage, Channel channel,
                             ChannelFutureListener channelFutureListener) {
        // if (channelFutureListener == null) {
        //     channelFutureListener = ChannelFutureListener.CLOSE;
        // }

        if (errorMessage != null) {
            int idx = errorMessage.indexOf("Exception: ");
            if (idx > -1) {
                errorMessage = errorMessage.substring(idx + 11);
            }
        }

        Packet packetError =
                Packet.newBuilder(packet.getId())
                        .setType(PacketType.ACK).setBody(Ack.newBuilder()
                        .setErrorCode(errorCode).setErrorMessage(errorMessage).build())
                        .build();
        write(channel, packetError, channelFutureListener);
    }
}
