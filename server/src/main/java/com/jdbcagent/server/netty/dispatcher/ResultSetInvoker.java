package com.jdbcagent.server.netty.dispatcher;

import com.jdbcagent.core.protocol.LobMsg;
import com.jdbcagent.core.protocol.Packet;
import com.jdbcagent.core.protocol.ResultSetMsg;
import com.jdbcagent.server.jdbc.LobStore;
import com.jdbcagent.server.jdbc.ResultSetServer;
import com.jdbcagent.server.netty.NettyUtils;
import io.netty.channel.ChannelHandlerContext;

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
        NettyUtils.ack(ctx.channel(), packet, null);
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

            NettyUtils.write(ctx.channel(), Packet.newBuilder(packet.getId())
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

            NettyUtils.write(ctx.channel(), Packet.newBuilder(packet.getId())
                            .setBody(ResultSetMsg.newBuilder().setRowSet(rowSet).build()).build(),
                    null);
        } else {
            throw new SQLException("No resultSet found");
        }
    }

    static void readLob(ChannelHandlerContext ctx, Packet packet) throws SQLException {
        LobMsg request = (LobMsg) packet.getMessage();
        if (request == null || request.getLobId() == null) {
            throw new SQLException("large object id is null");
        }
        long position = request.getPosition() == null ? 0L : request.getPosition();
        int length = request.getLength() == null ? 0 : request.getLength();
        LobStore.Chunk chunk = LobStore.read(request.getLobId(), position, length);
        NettyUtils.write(ctx.channel(), Packet.newBuilder(packet.getId())
                        .setBody(LobMsg.newBuilder()
                                .setLobId(request.getLobId())
                                .setPosition(position)
                                .setData(chunk.data)
                                .setEof(chunk.eof)
                                .build())
                        .build(),
                null);
    }

    static void closeLob(ChannelHandlerContext ctx, Packet packet) throws SQLException {
        LobMsg request = (LobMsg) packet.getMessage();
        if (request != null && request.getLobId() != null) {
            LobStore.delete(request.getLobId());
        }
        NettyUtils.ack(ctx.channel(), packet, null);
    }
}
