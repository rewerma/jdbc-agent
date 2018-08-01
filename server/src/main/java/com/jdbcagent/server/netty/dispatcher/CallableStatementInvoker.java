package com.jdbcagent.server.netty.dispatcher;

import com.jdbcagent.core.protocol.CallableStatementMsg;
import com.jdbcagent.core.protocol.Packet;
import com.jdbcagent.server.jdbc.CallableStatementServer;
import com.jdbcagent.server.jdbc.StatementServer;
import com.jdbcagent.server.netty.NettyUtils;
import org.jboss.netty.channel.ChannelHandlerContext;

import java.io.Serializable;
import java.sql.SQLException;

/**
 * JDBC-Agent server 端 callableStatement 调用
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class CallableStatementInvoker {
    /**
     * 关闭方法调用
     *
     * @param ctx
     * @param packet
     * @throws SQLException
     */
    static void close(ChannelHandlerContext ctx, Packet packet) throws SQLException {
        CallableStatementMsg callableStatementMsg = (CallableStatementMsg) packet.getMessage();
        if (callableStatementMsg != null && callableStatementMsg.getId() != null) {
            CallableStatementServer callableStatementServer =
                    (CallableStatementServer) StatementServer.STATEMENTS
                            .getIfPresent(callableStatementMsg.getId());
            if (callableStatementServer != null) {
                callableStatementServer.close();
            }
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
    static void callableStatementMethod(ChannelHandlerContext ctx, Packet packet)
            throws SQLException {
        CallableStatementMsg callableStatementMsg = (CallableStatementMsg) packet.getMessage();
        CallableStatementServer callableStatementServer =
                (CallableStatementServer) StatementServer.STATEMENTS
                        .getIfPresent(callableStatementMsg.getId());
        if (callableStatementServer != null) {
            Serializable response =
                    callableStatementServer.preparedStatementMethod(callableStatementMsg);
            NettyUtils.write(ctx.getChannel(),
                    Packet.newBuilder(packet.getId())
                            .setBody(CallableStatementMsg.newBuilder()
                                    .setResponse(response).build())
                            .build(),
                    null);
        } else {
            throw new SQLException("No callableStatement found");
        }
    }
}
