package com.jdbcagent.server.netty.handler;

import com.jdbcagent.core.protocol.Packet;
import com.jdbcagent.server.netty.ChannelIds;
import com.jdbcagent.server.netty.JdbcDispatch;
import com.jdbcagent.server.netty.NettyUtils;
import com.jdbcagent.server.netty.dispatcher.Dispatcher;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.RejectedExecutionException;

/**
 * JDBC-Agent server 端 rpc 入口
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class SessionHandler extends ChannelInboundHandlerAdapter {
    private final static Logger logger = LoggerFactory.getLogger(SessionHandler.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) throws Exception {
        final Packet packet = Packet.parse((byte[]) message);
        final Channel channel = ctx.channel();
        final Integer channelId = ChannelIds.id(channel);
        final ChannelHandlerContext context = ctx;
        try {
            JdbcDispatch.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Dispatcher.dispatch(channelId, context, packet);
                    } catch (Throwable exception) {
                        logger.error(exception.getMessage(), exception);
                        NettyUtils.error(packet, 400, exception.getMessage(), channel, null);
                    }
                }
            });
        } catch (RejectedExecutionException busy) {
            logger.error("jdbc worker busy", busy);
            NettyUtils.error(packet, 503, "server busy", channel, null);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        ctx.close();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        try {
            InetSocketAddress socketAddress = (InetSocketAddress) ctx.channel().remoteAddress();
            if (logger.isDebugEnabled()) {
                logger.debug("Client: " + socketAddress.getAddress() + " connected");
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        final Channel channel = ctx.channel();
        final Integer channelId = ChannelIds.id(channel);
        final ChannelHandlerContext context = ctx;
        final Packet packet = Packet.newBuilder().setType(Packet.PacketType.CHANNEL_CLOSE).build();
        Runnable closeConnections = new Runnable() {
            @Override
            public void run() {
                try {
                    // 链接关闭时同时断开数据链接
                    Dispatcher.dispatch(channelId, context, packet);
                } catch (Exception ex) {
                    logger.error(ex.getMessage(), ex);
                }
            }
        };
        try {
            JdbcDispatch.execute(closeConnections);
        } catch (RejectedExecutionException busy) {
            closeConnections.run();
        }

        InetSocketAddress socketAddress = (InetSocketAddress) channel.remoteAddress();
        if (socketAddress != null && logger.isDebugEnabled()) {
            logger.debug("Client: " + socketAddress.getAddress() + " closed");
        }
    }
}
