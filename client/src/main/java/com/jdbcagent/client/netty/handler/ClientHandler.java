package com.jdbcagent.client.netty.handler;

import com.jdbcagent.client.JdbcAgentDataSource;
import com.jdbcagent.client.netty.JdbcAgentNettyClient;
import com.jdbcagent.client.netty.NettyUtils;
import com.jdbcagent.client.netty.NettyUtils.NettyResponse;
import com.jdbcagent.core.protocol.ClientAuth;
import com.jdbcagent.core.protocol.Packet;
import com.jdbcagent.core.protocol.Packet.PacketType;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

import java.util.concurrent.locks.ReentrantLock;

/**
 * JDBC-Agent client netty clientHandler
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class ClientHandler extends SimpleChannelHandler {
    private JdbcAgentDataSource jdbcAgentDataSource;

    public ClientHandler(JdbcAgentDataSource jdbcAgentDataSource) {
        this.jdbcAgentDataSource = jdbcAgentDataSource;
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        // 连接后认证
        Packet packet = Packet.newBuilder()
                .incrementAndGetId()
                .setBody(ClientAuth.newBuilder()
                        .setNetReadTimeout(jdbcAgentDataSource.getTimeout())
                        .setNetWriteTimeout(jdbcAgentDataSource.getTimeout())
                                .setUsername("").setPassword("").build())
                        .build();

        NettyUtils.write(ctx.getChannel(), packet, null);
        super.channelConnected(ctx, e);

    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        ChannelBuffer buffer = (ChannelBuffer) e.getMessage();
        Packet packet = Packet.parse(buffer.readBytes(buffer.readableBytes()).array());

        if (packet.getType() == PacketType.CLIENT_AUTH) {
            //通过认证设置为已连接
            JdbcAgentNettyClient.connected = true;
        } else {
            NettyResponse nettyRes = NettyUtils.RESPONSE_MAP.get(packet.getId());
            if (nettyRes != null) {
                ReentrantLock lock = NettyUtils.lock;
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
        jdbcAgentDataSource.close();
    }
}
