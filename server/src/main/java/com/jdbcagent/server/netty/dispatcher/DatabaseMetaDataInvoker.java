package com.jdbcagent.server.netty.dispatcher;

import com.jdbcagent.core.protocol.DatabaseMetaDataMsg;
import com.jdbcagent.core.protocol.Packet;
import com.jdbcagent.server.jdbc.DatabaseMetaDataServer;
import com.jdbcagent.server.netty.NettyUtils;
import org.jboss.netty.channel.ChannelHandlerContext;

import java.io.Serializable;
import java.sql.SQLException;

/**
 * JDBC-Agent server 端 databaseMetaData 调用
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
class DatabaseMetaDataInvoker {
    /**
     * 公共方法调用
     *
     * @param ctx
     * @param packet
     * @throws SQLException
     */
    static void databaseMetaDataMethod(ChannelHandlerContext ctx, Packet packet) throws SQLException {
        DatabaseMetaDataMsg databaseMetaDataMsg = (DatabaseMetaDataMsg) packet.getMessage();
        DatabaseMetaDataServer databaseMetaDataServer =
                DatabaseMetaDataServer.DB_META_DATAS.getIfPresent(databaseMetaDataMsg.getId());
        if (databaseMetaDataServer != null) {
            Serializable response = databaseMetaDataServer.databaseMetaDataMethod(databaseMetaDataMsg);
            NettyUtils.write(ctx.getChannel(),
                    Packet.newBuilder(packet.getId())
                            .setBody(DatabaseMetaDataMsg.newBuilder()
                                    .setResponse(response).build())
                            .build(), null);
        } else {
            throw new SQLException("No databaseMetaData found");
        }
    }
}
