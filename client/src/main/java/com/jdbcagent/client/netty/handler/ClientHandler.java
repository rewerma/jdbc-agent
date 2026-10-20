package com.jdbcagent.client.netty.handler;

import com.jdbcagent.client.JdbcAgentDataSource;
import com.jdbcagent.client.netty.JdbcAgentNettyClient;
import com.jdbcagent.client.netty.NettyUtils;
import com.jdbcagent.client.netty.NettyUtils.NettyResponse;
import com.jdbcagent.core.protocol.Ack;
import com.jdbcagent.core.protocol.ClientAuth;
import com.jdbcagent.core.protocol.Packet;
import com.jdbcagent.core.protocol.Packet.PacketType;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.concurrent.locks.ReentrantLock;

/**
 * JDBC-Agent client netty clientHandler
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class ClientHandler extends ChannelInboundHandlerAdapter {
    private JdbcAgentDataSource jdbcAgentDataSource;

    public ClientHandler(JdbcAgentDataSource jdbcAgentDataSource) {
        this.jdbcAgentDataSource = jdbcAgentDataSource;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // 连接后认证。写在 TLS handler 之后，握手完成前的数据会先被加密再发出。
        Packet packet = Packet.newBuilder()
                .incrementAndGetId()
                .setType(PacketType.CLIENT_AUTH)
                .setBody(ClientAuth.newBuilder()
                        .setNetReadTimeout(jdbcAgentDataSource.getTimeout())
                        .setNetWriteTimeout(jdbcAgentDataSource.getTimeout())
                        .setUsername(jdbcAgentDataSource.getUsername())
                        .setPassword(jdbcAgentDataSource.getPassword())
                        .build())
                .build();

        NettyUtils.write(ctx.channel(), packet, null);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) throws Exception {
        Packet packet = Packet.parse((byte[]) message);

        if (packet.getType() == PacketType.CLIENT_AUTH) {
            ReentrantLock lock = NettyUtils.lock;
            lock.lock();
            try {
                Ack ack = packet.getMessage() instanceof Ack ? (Ack) packet.getMessage() : null;
                if (ack != null && ack.getErrorCode() == 0) {
                    JdbcAgentNettyClient.authError = null;
                    JdbcAgentNettyClient.connected = true;
                } else {
                    JdbcAgentNettyClient.connected = false;
                    JdbcAgentNettyClient.authError = ack == null || ack.getErrorMessage() == null
                            ? "authentication failed" : ack.getErrorMessage();
                }
                NettyUtils.CONNECTED.signalAll();
            } finally {
                lock.unlock();
            }
        } else {
            NettyResponse nettyRes = NettyUtils.RESPONSE_MAP.get(packet.getId());
            if (nettyRes != null) {
                ReentrantLock lock = NettyUtils.lock;
                lock.lock();
                try {
                    nettyRes.setPacket(packet);
                    if (nettyRes.getCondition() != null) {
                        nettyRes.getCondition().signal();
                    }
                } finally {
                    lock.unlock();
                }
            }
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        NettyUtils.failPending("connection closed");
        if (!jdbcAgentDataSource.isCurrentChannel(ctx.channel())) {
            return;
        }
        if (!jdbcAgentDataSource.reconnectAfterClose()) {
            jdbcAgentDataSource.close();
        }
    }
}
