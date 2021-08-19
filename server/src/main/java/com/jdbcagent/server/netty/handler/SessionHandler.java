package com.jdbcagent.server.netty.handler;

import com.jdbcagent.core.protocol.Packet;
import com.jdbcagent.server.netty.NettyUtils;
import com.jdbcagent.server.netty.dispatcher.ConnectionInvoker;
import com.jdbcagent.server.netty.dispatcher.Dispatcher;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * JDBC-Agent server 端 rpc 入口
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class SessionHandler extends SimpleChannelHandler {
    private final static Logger logger = LoggerFactory.getLogger(SessionHandler.class);

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        ChannelBuffer buffer = (ChannelBuffer) e.getMessage();
        Packet packet = Packet.parse(buffer.readBytes(buffer.readableBytes()).array());
        try {
            Dispatcher.dispatch(e.getChannel().getId(), ctx, packet);
        } catch (Throwable exception) {
            logger.error(exception.getMessage(), exception);
            NettyUtils.error(packet, 400, exception.getMessage(), ctx.getChannel(), null);
        }

        super.messageReceived(ctx, e);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        ctx.getChannel().close();
    }


    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        try {
            InetSocketAddress socketAddress = (InetSocketAddress) ctx.getChannel().getRemoteAddress();
            if (logger.isDebugEnabled()) {
                logger.debug("Client: " + socketAddress.getAddress() + " connected");
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
        }
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        try {
            //链接关闭是同时断开数据链接
            Dispatcher.dispatch(e.getChannel().getId(), ctx, Packet.newBuilder().setType(Packet.PacketType.CHANNEL_CLOSE).build());

            InetSocketAddress socketAddress = (InetSocketAddress) ctx.getChannel().getRemoteAddress();
            if (logger.isDebugEnabled()) {
                logger.debug("Client: " + socketAddress.getAddress() + " closed");
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
        }

    }
}
