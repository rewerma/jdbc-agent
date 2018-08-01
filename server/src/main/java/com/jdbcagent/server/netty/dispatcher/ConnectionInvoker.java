package com.jdbcagent.server.netty.dispatcher;

import com.jdbcagent.core.protocol.ConnectionMsg;
import com.jdbcagent.core.protocol.Packet;
import com.jdbcagent.server.jdbc.ConnectionServer;
import com.jdbcagent.server.netty.NettyUtils;
import org.jboss.netty.channel.ChannelHandlerContext;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * JDBC-Agent server 端 connection 调用
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class ConnectionInvoker {
    private static final ConcurrentHashMap<ChannelHandlerContext, Long> CONNECTIONS =
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
        CONNECTIONS.put(ctx, connectionId);
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
        Long connectionId = CONNECTIONS.get(ctx);
        if (connectionId != null) {
            ConnectionServer connectionServer = ConnectionServer.CONNECTIONS.get(connectionId);
            connectionServer.close();
        }
        CONNECTIONS.remove(ctx);
    }

    /**
     * 关闭方法调用
     *
     * @param ctx
     * @throws SQLException
     */
    public static void close(ChannelHandlerContext ctx, Packet packet) throws SQLException {
        closeConn(ctx);
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
