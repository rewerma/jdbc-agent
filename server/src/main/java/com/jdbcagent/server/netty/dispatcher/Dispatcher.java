package com.jdbcagent.server.netty.dispatcher;

import com.jdbcagent.core.protocol.Packet;
import org.jboss.netty.channel.ChannelHandlerContext;

import java.sql.SQLException;

/**
 * JDBC-Agent server 端调用分发类
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class Dispatcher {
    /**
     * 通过调用信息和数据源分发调用
     *
     * @param ctx    通道上下文
     * @param packet 数据包
     * @throws SQLException
     */
    public static void dispatch(ChannelHandlerContext ctx, Packet packet) throws SQLException {
        switch (packet.getType()) {
            case CONN_CONNECT:
                ConnectionInvoker.connect(ctx, packet);
                break;
            case CONN_CLOSE:
                ConnectionInvoker.close(ctx, packet);
                break;
            case CONN_METHOD:
                ConnectionInvoker.connMethod(ctx, packet);
                break;
            case CONN_SERIAL_METHOD:
                ConnectionInvoker.connSerialMethod(ctx, packet);
                break;

            case STMT_METHOD:
                StatementInvoker.statementMethod(ctx, packet);
                break;
            case STMT_CLOSE:
                StatementInvoker.close(ctx, packet);
                break;

            case PRE_STMT_METHOD:
                PreparedStatementInvoker.preparedStatementMethod(ctx, packet);
                break;
            case PRE_STMT_CLOSE:
                PreparedStatementInvoker.close(ctx, packet);
                break;

            case CLA_STMT_METHOD:
                CallableStatementInvoker.callableStatementMethod(ctx, packet);
                break;
            case CLA_STMT_CLOSE:
                CallableStatementInvoker.close(ctx, packet);
                break;

            case DB_METADATA_METHOD:
                DatabaseMetaDataInvoker.databaseMetaDataMethod(ctx, packet);
                break;

            case RS_CLOSE:
                ResultSetInvoker.close(ctx, packet);
                break;
            case RS_META_DATA:
                ResultSetInvoker.getMetaData(ctx, packet);
                break;
            case RS_FETCH_ROWS:
                ResultSetInvoker.fetchRows(ctx, packet);
                break;
        }
    }
}
