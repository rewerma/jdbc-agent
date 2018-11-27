package com.jdbcagent.server.netty.handler;

import com.jdbcagent.core.protocol.Packet;
import com.jdbcagent.server.config.Configuration;
import com.jdbcagent.server.netty.NettyUtils;
import com.jdbcagent.server.netty.dispatcher.ConnectionInvoker;
import com.jdbcagent.server.netty.dispatcher.Dispatcher;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * JDBC-Agent server 端 rpc 入口
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class SessionHandler extends SimpleChannelHandler {
    private static Logger logger = LoggerFactory.getLogger(SessionHandler.class);

    public static final ExecutorService executorService = Executors.newCachedThreadPool();

    @Override
    public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) throws Exception {
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                ChannelBuffer buffer = (ChannelBuffer) e.getMessage();
                Packet packet = null;
                try {
                    packet = Packet.parse(buffer.readBytes(buffer.readableBytes()).array(),
                            Configuration.getJdbcAgentCon().getJdbcAgent().getSerializeType());
                    Dispatcher.dispatch(ctx, packet);
                } catch (Exception exception) {
                    logger.error(exception.getMessage(), exception);
                    if (packet == null) {
                        packet = new Packet();
                        packet.setId(0L);
                    }
                    NettyUtils.error(packet, 400, exception.getMessage(), ctx.getChannel(), null);
                }
            }
        });
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
        ConnectionInvoker.closeConn(ctx);

        try {
            InetSocketAddress socketAddress = (InetSocketAddress) ctx.getChannel().getRemoteAddress();
            if (logger.isDebugEnabled()) {
                logger.debug("Client: " + socketAddress.getAddress() + " closed");
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
        }

    }
}
