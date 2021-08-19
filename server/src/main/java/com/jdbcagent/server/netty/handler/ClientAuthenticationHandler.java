package com.jdbcagent.server.netty.handler;

import com.jdbcagent.core.protocol.ClientAuth;
import com.jdbcagent.core.protocol.Packet;
import com.jdbcagent.server.netty.NettyUtils;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.timeout.IdleStateAwareChannelHandler;
import org.jboss.netty.handler.timeout.IdleStateEvent;
import org.jboss.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * JDBC-Agent server 客户端认证
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class ClientAuthenticationHandler extends SimpleChannelHandler {
    private static final Logger logger = LoggerFactory.getLogger(ClientAuthenticationHandler.class);
    private final int SUPPORTED_VERSION = 1;
    private final int defaultSubscriptorDisconnectIdleTimeout = 60 * 60 * 1000;

    public void messageReceived(final ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        ChannelBuffer buffer = (ChannelBuffer) e.getMessage();
        Packet packet = Packet.parse(buffer.readBytes(buffer.readableBytes()).array());
        switch (packet.getVersion()) {
            case SUPPORTED_VERSION:
            default:
                final ClientAuth clientAuth = (ClientAuth) packet.getMessage();
                // 验证账号密码
                if ("".equalsIgnoreCase(clientAuth.getUsername()) && "".equalsIgnoreCase(clientAuth.getPassword())) {
                }

                NettyUtils.ackAuth(ctx.getChannel(), packet, new ChannelFutureListener() {
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if (logger.isDebugEnabled()) {
                            logger.debug("remove unused channel handlers after authentication is done successfully.");
                        }
                        ctx.getPipeline().remove(ClientAuthenticationHandler.class.getName());

                        int readTimeout = defaultSubscriptorDisconnectIdleTimeout;
                        int writeTimeout = defaultSubscriptorDisconnectIdleTimeout;
                        if (clientAuth.getNetReadTimeout() > 0) {
                            readTimeout = clientAuth.getNetReadTimeout();
                        }
                        if (clientAuth.getNetWriteTimeout() > 0) {
                            writeTimeout = clientAuth.getNetWriteTimeout();
                        }

                        // millseconds.
                        IdleStateHandler idleStateHandler = new IdleStateHandler(NettyUtils.hashedWheelTimer,
                                readTimeout,
                                writeTimeout,
                                0,
                                TimeUnit.MILLISECONDS);
                        ctx.getPipeline().addBefore(SessionHandler.class.getName(),
                                IdleStateHandler.class.getName(),
                                idleStateHandler);

                        IdleStateAwareChannelHandler idleStateAwareChannelHandler = new IdleStateAwareChannelHandler() {

                            public void channelIdle(ChannelHandlerContext ctx, IdleStateEvent e) throws Exception {
                                logger.warn("channel:{} idle timeout exceeds, close channel to save server resources...",
                                        ctx.getChannel());
                                ctx.getChannel().close();
                            }

                        };
                        ctx.getPipeline().addBefore(SessionHandler.class.getName(),
                                IdleStateAwareChannelHandler.class.getName(),
                                idleStateAwareChannelHandler);
                    }

                });
                break;
        }
    }
}
