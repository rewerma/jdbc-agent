package com.jdbcagent.client.netty.handler;

import com.jdbcagent.client.JdbcAgentDataSource;
import com.jdbcagent.client.netty.JdbcAgentNettyClient;
import com.jdbcagent.client.netty.NettyUtils;
import com.jdbcagent.client.netty.NettyUtils.NettyResponse;
import com.jdbcagent.client.util.SerializeUtil;
import com.jdbcagent.core.protocol.Packet;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * JDBC-Agent client netty clientHandler
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class ClientHandler extends SimpleChannelHandler {
    private JdbcAgentNettyClient jdbcAgentNettyClient;
    private int timeout;
    private AtomicBoolean connected;
    private NettyUtils nettyUtils;

    public ClientHandler(JdbcAgentNettyClient jdbcAgentNettyClient, int timeout, AtomicBoolean connected, NettyUtils nettyUtils) {
        this.jdbcAgentNettyClient = jdbcAgentNettyClient;
        this.timeout = timeout;
        this.connected = connected;
        this.nettyUtils = nettyUtils;
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        // 连接后认证
        String packet = "93" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()) +
                "|AA" + "" +
                "|AD" + "" +
                "|XW" + timeout +
                "|XR" + timeout +
                "|XV1";

        nettyUtils.write(ctx.getChannel(), packet.getBytes(StandardCharsets.UTF_8), null);
        super.channelConnected(ctx, e);

    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        ChannelBuffer buffer = (ChannelBuffer) e.getMessage();
        byte[] body = buffer.readBytes(buffer.readableBytes()).array();

        if (!connected.get()) {
            //如果未连接说明是认证返回的数据包
            String ackPacket = new String(body, StandardCharsets.UTF_8);
            if (!ackPacket.startsWith("64")) {
                throw new RuntimeException("error ack from jdbc agent server ");
            }
            String[] packetItems = ackPacket.split("\\|");
            for (String packetItem : packetItems) {
                if (packetItem.startsWith("AS")) {
                    SerializeUtil.serializeType = Packet.SerializeType.valueOf(packetItem.substring(2));
                }
            }
            //通过认证设置为已连接
            connected.set(true);
        } else {
            Packet packet = Packet.parse(body, SerializeUtil.serializeType);
            NettyResponse nettyRes = nettyUtils.RESPONSE_MAP.get(packet.getId());
            if (nettyRes != null) {
                ReentrantLock lock = nettyUtils.lock;
                nettyRes.setPacket(packet);
                try {
                    lock.lock();
                    nettyRes.getCondition().signal();
                } finally {
                    lock.unlock();
                }
            }
        }

        super.messageReceived(ctx, e);
    }


    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        //如果连接断开了关闭释放客户端连接
        jdbcAgentNettyClient.stop();
    }
}
