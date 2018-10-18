package com.jdbcagent.server.netty.handler;

import com.jdbcagent.server.config.Configuration;
import com.jdbcagent.server.netty.NettyUtils;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.timeout.IdleStateAwareChannelHandler;
import org.jboss.netty.handler.timeout.IdleStateEvent;
import org.jboss.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * JDBC-Agent server 客户端认证
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class ClientAuthenticationHandler extends SimpleChannelHandler {
    private static final Logger logger = LoggerFactory.getLogger(ClientAuthenticationHandler.class);
    private static final int SUPPORTED_VERSION = 1;
    private static final int defaultDisconnectIdleTimeout = 60 * 60 * 1000;

    public void messageReceived(final ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        try {
            ChannelBuffer buffer = (ChannelBuffer) e.getMessage();
            byte[] body = buffer.readBytes(buffer.readableBytes()).array();
            String packet = new String(body, StandardCharsets.UTF_8);

            if (!packet.startsWith("93")) {
                throw new RuntimeException();
            }
            String[] packetItems = packet.split("\\|");
            int version = 1;
            String username = "";
            String password = "";
            int pwriteTimeout = defaultDisconnectIdleTimeout;
            int preadTimeout = defaultDisconnectIdleTimeout;
            for (String packetItem : packetItems) {
                if (packetItem.startsWith("XV")) {
                    version = Integer.parseInt(packetItem.substring(2));
                } else if (packetItem.startsWith("AA")) {
                    username = packetItem.substring(2);
                } else if (packetItem.startsWith("AD")) {
                    password = packetItem.substring(2);
                } else if (packetItem.startsWith("XW")) {
                    pwriteTimeout = Integer.parseInt(packetItem.substring(2));
                } else if (packetItem.startsWith("XR")) {
                    preadTimeout = Integer.parseInt(packetItem.substring(2));
                }
            }
            final int fwriteTimeout = pwriteTimeout;
            final int rreadTimeout = preadTimeout;
            switch (version) {
                case SUPPORTED_VERSION:
                default:
                    // 验证账号密码
                    if ("".equalsIgnoreCase(username) && "".equalsIgnoreCase(password)) {
                    }

                    String ackPacket = "64" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()) +
                            "|AS" + Configuration.getJdbcAgentCon().getJdbcAgent().getSerialize(); //将序列化方式返回

                    NettyUtils.ackAuth(ctx.getChannel(), ackPacket.getBytes(StandardCharsets.UTF_8), new ChannelFutureListener() {
                        public void operationComplete(ChannelFuture future) throws Exception {
                            if (logger.isDebugEnabled()) {
                                logger.debug("remove unused channel handlers after authentication is done successfully.");
                            }
                            ctx.getPipeline().remove(ClientAuthenticationHandler.class.getName());

                            int readTimeout = defaultDisconnectIdleTimeout;
                            int writeTimeout = defaultDisconnectIdleTimeout;
                            if (rreadTimeout > 0) {
                                readTimeout = rreadTimeout;
                            }
                            if (fwriteTimeout > 0) {
                                writeTimeout = fwriteTimeout;
                            }

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
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            NettyUtils.ackAuth(ctx.getChannel(), "941".getBytes(StandardCharsets.UTF_8), null);
        }
    }
}
