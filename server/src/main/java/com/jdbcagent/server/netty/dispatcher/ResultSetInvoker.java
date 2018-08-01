package com.jdbcagent.server.netty.dispatcher;

import com.jdbcagent.core.protocol.Packet;
import com.jdbcagent.core.protocol.ResultSetMsg;
import com.jdbcagent.server.jdbc.ResultSetServer;
import com.jdbcagent.server.netty.NettyUtils;
import org.jboss.netty.channel.ChannelHandlerContext;

import javax.sql.RowSet;
import java.sql.SQLException;

/**
 * JDBC-Agent server 端 resultSet 调用
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
class ResultSetInvoker {
    /**
     * 关闭方法调用
     *
     * @param ctx
     * @param packet
     * @throws SQLException
     */
    static void close(ChannelHandlerContext ctx, Packet packet) throws SQLException {
        ResultSetMsg resultSetMsg = (ResultSetMsg) packet.getMessage();
        if (resultSetMsg != null && resultSetMsg.getId() != null) {
            ResultSetServer resultSetServer = ResultSetServer.RESULTSETS.getIfPresent(resultSetMsg.getId());
            if (resultSetServer != null) {
                resultSetServer.close();
            }
        }
        NettyUtils.ack(ctx.getChannel(), packet, null);
    }

    /**
     * 获取元数据
     *
     * @param ctx
     * @param packet
     * @throws SQLException
     */
    static void getMetaData(ChannelHandlerContext ctx, Packet packet) throws SQLException {
        ResultSetMsg resultSetMsg = (ResultSetMsg) packet.getMessage();
        ResultSetServer resultSetServer = ResultSetServer.RESULTSETS.getIfPresent(resultSetMsg.getId());
        if (resultSetServer != null) {
            RowSet rowSet = resultSetServer.getMetaData();

            NettyUtils.write(ctx.getChannel(), Packet.newBuilder(packet.getId())
                            .setBody(ResultSetMsg.newBuilder().setRowSet(rowSet).build()).build(),
                    null);
        } else {
            throw new SQLException("No resultSet found");
        }
    }

    /**
     * 获取分批数据
     *
     * @param ctx
     * @param packet
     * @throws SQLException
     */
    static void fetchRows(ChannelHandlerContext ctx, Packet packet) throws SQLException {
        ResultSetMsg resultSetMsg = (ResultSetMsg) packet.getMessage();
        ResultSetServer resultSetServer = ResultSetServer.RESULTSETS.getIfPresent(resultSetMsg.getId());
        if (resultSetServer != null) {
            int batchSize = resultSetMsg.getBatchSize() == null ? 500 : resultSetMsg.getBatchSize();
            RowSet rowSet = resultSetServer.fetchRows(batchSize);

            NettyUtils.write(ctx.getChannel(), Packet.newBuilder(packet.getId())
                            .setBody(ResultSetMsg.newBuilder().setRowSet(rowSet).build()).build(),
                    null);
        } else {
            throw new SQLException("No resultSet found");
        }
    }
}
