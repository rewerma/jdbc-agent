package com.jdbcagent.server.netty;

import com.jdbcagent.core.protocol.Ack;
import com.jdbcagent.core.protocol.Packet;
import com.jdbcagent.core.protocol.Packet.PacketType;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

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
