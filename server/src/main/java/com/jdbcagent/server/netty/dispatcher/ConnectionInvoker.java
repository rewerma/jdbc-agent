package com.jdbcagent.server.netty.dispatcher;

import com.jdbcagent.core.protocol.ConnectionMsg;
import com.jdbcagent.core.protocol.Packet;
import com.jdbcagent.server.jdbc.ConnectionServer;
import com.jdbcagent.server.netty.NettyUtils;
import org.jboss.netty.channel.ChannelHandlerContext;

import java.io.Serializable;
import java.sql.SQLException;

/**
 * JDBC-Agent server 端 connection 调用
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class ConnectionInvoker {

    /**
     * 创建连接方法调用
     *
     * @param ctx
     * @param packet
     * @throws SQLException
     */
    static void connect(Integer channelId, ChannelHandlerContext ctx, Packet packet) throws SQLException {
        ConnectionMsg connectMsg = (ConnectionMsg) packet.getMessage();
        ConnectionServer connectionServer = new ConnectionServer();
        long connectionId = connectionServer.connect(channelId, connectMsg.getCatalog(),
                connectMsg.getUsername(), connectMsg.getPassword());
        NettyUtils.write(ctx.getChannel(), Packet.newBuilder(packet.getId())
                        .setBody(ConnectionMsg.newBuilder()
                                .setId(connectionId).build()).build(),
                null);
    }

    static void channelClose(Integer channelId) throws SQLException {
        ConnectionServer.close(channelId);
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
