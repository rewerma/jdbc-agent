package com.jdbcagent.server.netty.dispatcher;

import com.jdbcagent.core.protocol.ConnectionMsg;
import com.jdbcagent.core.protocol.Packet;
import com.jdbcagent.server.jdbc.ConnectionServer;
import com.jdbcagent.server.netty.NettyUtils;
import org.jboss.netty.channel.ChannelHandlerContext;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * JDBC-Agent server 端 connection 调用
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class ConnectionInvoker {
    private static final ConcurrentHashMap<ChannelHandlerContext, Map<Long, Long>> CONNECTIONS =
            new ConcurrentHashMap<>();

    /**
     * 创建连接方法调用
     *
     * @param ctx
     * @param packet
     * @throws SQLException
     */
    static void connect(ChannelHandlerContext ctx, Packet packet) throws SQLException {
        ConnectionMsg connectMsg = (ConnectionMsg) packet.getMessage();
        ConnectionServer connectionServer = new ConnectionServer();
        long connectionId = connectionServer.connect(connectMsg.getCatalog(),
                connectMsg.getUsername(), connectMsg.getPassword());

        Map<Long, Long> ids = CONNECTIONS.get(ctx);
        if (ids == null) {
            synchronized (ConnectionInvoker.class) {
                ids = CONNECTIONS.get(ctx);
                if (ids == null) {
                    ids = new ConcurrentHashMap<>();
                    CONNECTIONS.put(ctx, ids);
                }
            }
        }
        ids.put(connectionId, connectionId);

        NettyUtils.write(ctx.getChannel(), Packet.newBuilder(packet.getId())
                        .setBody(ConnectionMsg.newBuilder()
                                .setId(connectionId).build()).build(),
                null);
    }

    /**
     * 关闭conn
     *
     * @param ctx
     * @throws SQLException
     */
    public static void closeConn(ChannelHandlerContext ctx) throws SQLException {
        Map<Long, Long> connectionIds = CONNECTIONS.remove(ctx);
        for (Long connectionId : connectionIds.keySet()) {
            if (connectionId != null) {
                ConnectionServer connectionServer = ConnectionServer.CONNECTIONS.get(connectionId);
                if (connectionServer != null) {
                    connectionServer.close();
                }
            }
        }
    }

    /**
     * 关闭方法调用
     *
     * @param ctx
     * @throws SQLException
     */
    public static void close(ChannelHandlerContext ctx, Packet packet) throws SQLException {
        ConnectionMsg message = (ConnectionMsg) packet.getBody();
        if (message != null && message.getId() != null) {
            ConnectionServer connectionServer = ConnectionServer.CONNECTIONS.get(message.getId());
            connectionServer.close();
        }
        NettyUtils.ack(ctx.getChannel(), packet, null);
    }

    /**
     * 公共方法调用
     *
     * @param ctx
     * @param packet
     * @throws SQLException
     */
    static void connMethod(ChannelHandlerContext ctx, Packet packet) throws SQLException {
        ConnectionMsg connectMsg = (ConnectionMsg) packet.getMessage();
        ConnectionServer connectionServer = ConnectionServer.CONNECTIONS.get(connectMsg.getId());
        Serializable response = connectionServer.connMethod(connectMsg);
        NettyUtils.write(ctx.getChannel(),
                Packet.newBuilder(packet.getId())
                        .setBody(ConnectionMsg.newBuilder()
                                .setResponse(response)
                                .setWarnings(connectionServer.getWarnings()).build())
                        .build(), null);
    }

    /**
     * 可序列化conn公共方法调用
     *
     * @param ctx
     * @param packet
     * @throws SQLException
     */
    static void connSerialMethod(ChannelHandlerContext ctx, Packet packet) throws SQLException {
        ConnectionMsg connectMsg = (ConnectionMsg) packet.getMessage();
        ConnectionServer connectionServer = ConnectionServer.CONNECTIONS.get(connectMsg.getId());
        Serializable response = connectionServer.connMethod(connectMsg);
        NettyUtils.write(ctx.getChannel(),
                Packet.newBuilder(packet.getId())
                        .setBody(ConnectionMsg.newBuilder().setResponse(response)
                                .setSerialConnection(connectionServer.getSerialConnection())
                                .setWarnings(connectionServer.getWarnings()).build())
                        .build(), null);
    }
}
