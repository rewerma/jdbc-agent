package com.jdbcagent.server.netty.dispatcher;

import com.jdbcagent.core.protocol.Packet;
import com.jdbcagent.core.protocol.PreparedStatementMsg;
import com.jdbcagent.server.jdbc.PreparedStatementServer;
import com.jdbcagent.server.jdbc.StatementServer;
import com.jdbcagent.server.netty.NettyUtils;
import org.jboss.netty.channel.ChannelHandlerContext;

import java.io.Serializable;
import java.sql.SQLException;

/**
 * JDBC-Agent server 端 preparedStatement 调用
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class PreparedStatementInvoker {
    /**
     * 关闭方法调用
     *
     * @param ctx
     * @param packet
     * @throws SQLException
     */
    static void close(ChannelHandlerContext ctx, Packet packet) throws SQLException {
        PreparedStatementMsg preparedStatementMsg = (PreparedStatementMsg) packet.getMessage();
        if (preparedStatementMsg != null && preparedStatementMsg.getId() != null) {
            PreparedStatementServer preparedStatementServer =
                    (PreparedStatementServer) StatementServer.STATEMENTS
                            .getIfPresent(preparedStatementMsg.getId());
            if (preparedStatementServer != null) {
                preparedStatementServer.close();
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
    static void preparedStatementMethod(ChannelHandlerContext ctx, Packet packet)
            throws SQLException {
        PreparedStatementMsg preparedStatementMsg = (PreparedStatementMsg) packet.getMessage();
        PreparedStatementServer preparedStatementServer =
                (PreparedStatementServer) StatementServer.STATEMENTS
                        .getIfPresent(preparedStatementMsg.getId());
        if (preparedStatementServer != null) {
            Serializable response =
                    preparedStatementServer.preparedStatementMethod(preparedStatementMsg);
            NettyUtils.write(ctx.getChannel(),
                    Packet.newBuilder(packet.getId())
                            .setBody(PreparedStatementMsg.newBuilder().setResponse(response).build())
                            .build(), null);
        } else {
            throw new SQLException("No preparedStatement found");
        }
    }
}
