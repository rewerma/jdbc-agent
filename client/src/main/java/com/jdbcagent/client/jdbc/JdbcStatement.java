package com.jdbcagent.client.jdbc;

import com.jdbcagent.client.JdbcAgentConnector;
import com.jdbcagent.client.uitl.SerializeUtil;
import com.jdbcagent.core.protocol.Packet;
import com.jdbcagent.core.protocol.Packet.PacketType;
import com.jdbcagent.core.protocol.StatementMsg;
import com.jdbcagent.core.protocol.StatementMsg.Method;

import java.io.Serializable;
import java.sql.*;

/**
 * JDBC-Agent client jdbc connection impl
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class JdbcStatement implements Statement {
    long remoteId;                                          // 远程statement id

    private Connection conn;                                // connection

    private final JdbcAgentConnector jdbcAgentConnector;    // tcp连接器

    private JdbcResultSet results;                          // resultSet结果集

    /**
     * statement 构造方法
     *
     * @param conn               connection
     * @param jdbcAgentConnector tcp连接器
     * @param remoteId           远程statementId
     */
    JdbcStatement(Connection conn, JdbcAgentConnector jdbcAgentConnector, long remoteId) {
        this.conn = conn;
        this.remoteId = remoteId;
        this.jdbcAgentConnector = jdbcAgentConnector;
    }

    /**
     * Statement方法远程调用, 无参数
     *
     * @param method 方法名
     * @return 可序列化返回值
     * @throws SQLException
     */
    private Serializable invokeStatementMethod(StatementMsg.Method method) throws SQLException {
        return invokeStatementMethod(method, new Serializable[0]);
    }

    /**
     * Statement方法远程调用, 带参数
     *
     * @param method 方法名
     * @param params 方法参数
     * @return 可序列化返回值
     * @throws SQLException
     */
    private Serializable invokeStatementMethod(StatementMsg.Method method, Serializable[] params)
            throws SQLException {
        synchronized (jdbcAgentConnector) {
            Packet responsePacket =
                    Packet.parse(
                            jdbcAgentConnector.write(
                                    Packet.newBuilder()
                                            .incrementAndGetId()
                                            .setType(PacketType.STMT_METHOD)
                                            .setBody(StatementMsg.newBuilder().setId(remoteId)
                                                    .setMethod(method).setParams(params).build())
                                            .build()), SerializeUtil.serializeType);
            return ((StatementMsg) responsePacket.getBody()).getResponse();
        }
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        long resultSetId =
                (Long) invokeStatementMethod(Method.executeQuery, new Serializable[]{sql});
        this.results = new JdbcResultSet(jdbcAgentConnector, resultSetId);
        return this.results;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return (Integer) invokeStatementMethod(Method.executeUpdate, new Serializable[]{sql});
    }

    @Override
    public void close() throws SQLException {
        synchronized (jdbcAgentConnector) {
            Packet packet = Packet.newBuilder()
                    .incrementAndGetId()
                    .setType(PacketType.STMT_CLOSE)
                    .setBody(StatementMsg.newBuilder().setId(remoteId).build()).build();
            Packet.parse(jdbcAgentConnector.write(packet), SerializeUtil.serializeType).getAck();
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return (Integer) invokeStatementMethod(Method.getMaxFieldSize);
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        invokeStatementMethod(Method.setMaxFieldSize, new Serializable[]{max});
    }

    @Override
    public int getMaxRows() throws SQLException {
        return (Integer) invokeStatementMethod(Method.getMaxRows);
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        invokeStatementMethod(Method.setMaxRows, new Serializable[]{max});
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        invokeStatementMethod(Method.setEscapeProcessing, new Serializable[]{enable});
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return (Integer) invokeStatementMethod(Method.getQueryTimeout);
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        invokeStatementMethod(Method.setQueryTimeout, new Serializable[]{seconds});
    }

    @Override
    public void cancel() throws SQLException {
        invokeStatementMethod(Method.cancel);
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        String warnMsg = (String) invokeStatementMethod(Method.getWarnings);
        if (warnMsg != null) {
            return new SQLWarning(warnMsg);
        } else {
            return null;
        }
    }

    @Override
    public void clearWarnings() throws SQLException {
        invokeStatementMethod(Method.clearWarnings);
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        invokeStatementMethod(Method.setCursorName, new Serializable[]{name});
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return (Boolean) invokeStatementMethod(Method.execute, new Serializable[]{sql});
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return this.results;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return (Integer) invokeStatementMethod(Method.getUpdateCount);
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return (Boolean) invokeStatementMethod(Method.getMoreResults);
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        invokeStatementMethod(Method.setFetchDirection, new Serializable[]{direction});
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return (Integer) invokeStatementMethod(Method.getFetchDirection);
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        invokeStatementMethod(Method.setFetchSize, new Serializable[]{rows});
    }

    @Override
    public int getFetchSize() throws SQLException {
        return (Integer) invokeStatementMethod(Method.getFetchSize);
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return (Integer) invokeStatementMethod(Method.getResultSetConcurrency);
    }

    @Override
    public int getResultSetType() throws SQLException {
        return (Integer) invokeStatementMethod(Method.getResultSetType);
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        invokeStatementMethod(Method.addBatch, new Serializable[]{sql});
    }

    @Override
    public void clearBatch() throws SQLException {
        invokeStatementMethod(Method.clearBatch);
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return (int[]) invokeStatementMethod(Method.executeBatch);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.conn;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return (Boolean) invokeStatementMethod(Method.getMoreResults, new Serializable[]{current});
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return (Integer) invokeStatementMethod(Method.executeUpdate,
                new Serializable[]{sql, autoGeneratedKeys});
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return (Integer) invokeStatementMethod(Method.executeUpdate,
                new Serializable[]{sql, columnIndexes});
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return (Integer) invokeStatementMethod(Method.executeUpdate,
                new Serializable[]{sql, columnNames});
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return (Boolean) invokeStatementMethod(Method.execute,
                new Serializable[]{sql, autoGeneratedKeys});
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return (Boolean) invokeStatementMethod(Method.execute,
                new Serializable[]{sql, columnIndexes});
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return (Boolean) invokeStatementMethod(Method.execute,
                new Serializable[]{sql, columnNames});
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return (Integer) invokeStatementMethod(Method.getResultSetHoldability);
    }

    @Override
    public boolean isClosed() throws SQLException {
        return (Boolean) invokeStatementMethod(Method.isClosed);
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        invokeStatementMethod(Method.setPoolable, new Serializable[]{poolable});
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return (Boolean) invokeStatementMethod(Method.isPoolable);
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        invokeStatementMethod(Method.closeOnCompletion);
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return (Boolean) invokeStatementMethod(Method.isCloseOnCompletion);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }
}
