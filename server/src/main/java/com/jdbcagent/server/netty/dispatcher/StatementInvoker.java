package com.jdbcagent.server.netty.dispatcher;

import com.jdbcagent.core.protocol.Packet;
import com.jdbcagent.core.protocol.StatementMsg;
import com.jdbcagent.server.jdbc.StatementServer;
import com.jdbcagent.server.netty.NettyUtils;
import org.jboss.netty.channel.ChannelHandlerContext;

import java.io.Serializable;
import java.sql.SQLException;

/**
 * JDBC-Agent server 端 statement 调用
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
class StatementInvoker {
    /**
     * 关闭方法调用
     *
     * @param ctx
     * @param packet
     * @throws SQLException
     */
    static void close(ChannelHandlerContext ctx, Packet packet) throws SQLException {
        StatementMsg statementMsg = (StatementMsg) packet.getMessage();
        if (statementMsg != null && statementMsg.getId() != null) {
            StatementServer statementServer = StatementServer.STATEMENTS.getIfPresent(statementMsg.getId());
            if (statementServer != null) {
                statementServer.close();
            }
        }
        NettyUtils.ack(ctx.getChannel(), packet, null);
    }

    /**
     * 工方法调用
     *
     * @param ctx
     * @param packet
     * @throws SQLException
     */
    static void statementMethod(ChannelHandlerContext ctx, Packet packet) throws SQLException {
        StatementMsg statementMsg = (StatementMsg) packet.getMessage();
        StatementServer statementServer = StatementServer.STATEMENTS.getIfPresent(statementMsg.getId());
        if (statementServer != null) {
            Serializable response = statementServer.statementMethod(statementMsg);
            NettyUtils.write(ctx.getChannel(),
                    Packet.newBuilder(packet.getId())
                            .setBody(StatementMsg.newBuilder().setResponse(response).build())
                            .build(),
                    null);
        } else {
            throw new SQLException("No statement found");
        }
    }
}
