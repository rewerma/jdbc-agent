package com.jdbcagent.server.netty.handler;

import com.jdbcagent.core.protocol.Ack;
import com.jdbcagent.core.protocol.ClientAuth;
import com.jdbcagent.core.protocol.Packet;
import com.jdbcagent.server.config.JdbcAgentConf;
import com.jdbcagent.server.netty.NettyUtils;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * JDBC-Agent server 客户端认证
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class ClientAuthenticationHandler extends ChannelInboundHandlerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(ClientAuthenticationHandler.class);
    private final int SUPPORTED_VERSION = 1;
    private final int defaultSubscriptorDisconnectIdleTimeout = 60 * 60 * 1000;

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object message) throws Exception {
        Packet packet;
        try {
            packet = Packet.parse((byte[]) message);
        } catch (RuntimeException ex) {
            logger.warn("reject handshake, packet decode failed");
            ctx.close();
            return;
        }
        if (packet == null || packet.getVersion() != SUPPORTED_VERSION
                || packet.getType() != Packet.PacketType.CLIENT_AUTH
                || !(packet.getMessage() instanceof ClientAuth)) {
            reject(ctx, packet, "invalid handshake");
            return;
        }
        final ClientAuth clientAuth = (ClientAuth) packet.getMessage();
        if (!JdbcAgentConf.authenticate(clientAuth.getUsername(), clientAuth.getPassword())) {
            logger.warn("reject handshake for user {}", displayUser(clientAuth.getUsername()));
            reject(ctx, packet, "invalid username or password");
            return;
        }

        NettyUtils.ackAuth(ctx.channel(), packet, new ChannelFutureListener() {
            public void operationComplete(ChannelFuture future) throws Exception {
                if (logger.isDebugEnabled()) {
                    logger.debug("remove unused channel handlers after authentication is done successfully.");
                }
                ctx.pipeline().remove(ClientAuthenticationHandler.class.getName());

                int readTimeout = defaultSubscriptorDisconnectIdleTimeout;
                int writeTimeout = defaultSubscriptorDisconnectIdleTimeout;
                if (clientAuth.getNetReadTimeout() > 0) {
                    readTimeout = clientAuth.getNetReadTimeout();
                }
                if (clientAuth.getNetWriteTimeout() > 0) {
                    writeTimeout = clientAuth.getNetWriteTimeout();
                }

                // millseconds.
                IdleStateHandler idleStateHandler = new IdleStateHandler(readTimeout, writeTimeout, 0, TimeUnit.MILLISECONDS);
                ctx.pipeline().addBefore(SessionHandler.class.getName(),
                        IdleStateHandler.class.getName(),
                        idleStateHandler);

                ChannelInboundHandlerAdapter idleStateAwareChannelHandler = new ChannelInboundHandlerAdapter() {
                    @Override
                    public void userEventTriggered(ChannelHandlerContext idleCtx, Object event) throws Exception {
                        if (event instanceof IdleStateEvent) {
                            logger.warn("channel:{} idle timeout exceeds, close channel to save server resources...",
                                    idleCtx.channel());
                            idleCtx.close();
                            return;
                        }
                        super.userEventTriggered(idleCtx, event);
                    }
                };
                ctx.pipeline().addBefore(SessionHandler.class.getName(),
                        "idleClose",
                        idleStateAwareChannelHandler);
            }
        });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.warn("reject handshake, channel error");
        ctx.close();
    }

    private void reject(ChannelHandlerContext ctx, Packet packet, String message) {
        if (packet != null && packet.getId() != null) {
            Packet error = Packet.newBuilder(packet.getId())
                    .setType(Packet.PacketType.CLIENT_AUTH)
                    .setBody(Ack.newBuilder().setErrorCode(401).setErrorMessage(message).build())
                    .build();
            NettyUtils.write(ctx.channel(), error, ChannelFutureListener.CLOSE);
            return;
        }
        ctx.close();
    }

    private static String displayUser(String username) {
        if (username == null) {
            return "";
        }
        return username.length() <= 64 ? username : username.substring(0, 64);
    }
}
