package com.jdbcagent.client.jdbc;

import com.jdbcagent.client.JdbcAgentConnector;
import com.jdbcagent.client.uitl.SerializeUtil;
import com.jdbcagent.core.protocol.ConnectionMsg;
import com.jdbcagent.core.protocol.ConnectionMsg.Method;
import com.jdbcagent.core.protocol.Packet;
import com.jdbcagent.core.protocol.Packet.PacketType;
import com.jdbcagent.core.support.serial.SerialConnection;
import com.jdbcagent.core.support.serial.SerialSavepoint;

import java.io.Serializable;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * JDBC-Agent client jdbc connection impl
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class JdbcConnection implements Connection {
    private long remoteId;                                  // 远程connection id

    private final JdbcAgentConnector jdbcAgentConnector;    // tcp连接器

    private SerialConnection serialConnection;              // 可序列化的connection信息

    private boolean connected;                              // 是否连接

    private String warnings;                                // 警告信息

    /**
     * Connection 构造方法
     *
     * @param jdbcAgentConnector 连接器
     * @param catalog            目录名
     * @param username           用户名
     * @param password           密码
     * @throws SQLException
     */
    public JdbcConnection(JdbcAgentConnector jdbcAgentConnector, String catalog,
                          String username, String password) throws SQLException {
        this.jdbcAgentConnector = jdbcAgentConnector;

        Packet responsePacket =
                Packet.parse(jdbcAgentConnector.write(Packet
                        .newBuilder()
                        .incrementAndGetId()
                        .setType(PacketType.CONN_CONNECT).setBody(ConnectionMsg
                                .newBuilder()
                                .setCatalog(catalog)
                                .setUsername(username)
                                .setPassword(password).build())
                        .build()), SerializeUtil.serializeType);
        ConnectionMsg connectMsg = (ConnectionMsg) responsePacket.getBody();
        remoteId = connectMsg.getId();
        connected = true;
    }

    /**
     * 获取可序列化的Connection对象信息
     * 如果不存在(第一次访问)则进行远程调用
     *
     * @param method 所调方法
     * @return 可序列化Connection
     * @throws SQLException
     */
    public SerialConnection getSerialConnection(Method method) throws SQLException {
        if (serialConnection == null) {
            synchronized (jdbcAgentConnector) {
                try {
                    Packet responsePacket =
                            Packet.parse(jdbcAgentConnector.write(Packet.newBuilder().incrementAndGetId()
                                    .setType(PacketType.CONN_SERIAL_METHOD).setBody(ConnectionMsg.newBuilder()
                                            .setId(remoteId).setMethod(method).setParams(new Serializable[0]).build())
                                    .build()), SerializeUtil.serializeType);
                    ConnectionMsg response = (ConnectionMsg) responsePacket.getBody();
                    warnings = response.getWarnings();
                    serialConnection = response.getSerialConnection();
                } catch (Exception e) {
                    checkConnection(e);
                    throw new SQLException(e);
                }
            }
        }
        return serialConnection;
    }

    /**
     * 获取可序列化的Connection对象信息
     * 如果是第一次调用需要调用获取可序列化conn对象
     * 否则只远程调用不用获取可序列化conn对象
     *
     * @param method 所调方法
     * @param params 方法参数
     * @return 可序列化Connection
     * @throws SQLException
     */
    public SerialConnection getSerialConnection(Method method, Serializable... params) throws SQLException {
        if (serialConnection == null) {
            try {
                Packet responsePacket =
                        Packet.parse(jdbcAgentConnector.write(Packet.newBuilder().incrementAndGetId()
                                .setType(PacketType.CONN_SERIAL_METHOD).setBody(ConnectionMsg.newBuilder()
                                        .setId(remoteId).setMethod(method).setParams(params).build())
                                .build()), SerializeUtil.serializeType);
                ConnectionMsg response = (ConnectionMsg) responsePacket.getBody();
                warnings = response.getWarnings();
                serialConnection = response.getSerialConnection();
            } catch (Exception e) {
                checkConnection(e);
                throw new SQLException(e);
            }
        } else {
            invokeConnMethod(method, params);
        }
        return serialConnection;
    }

    /**
     * Connection方法远程调用, 无参数
     *
     * @param method 方法名
     * @return 可序列化返回值
     * @throws SQLException
     */
    private Serializable invokeConnMethod(Method method) throws SQLException {
        return invokeConnMethod(method, new Serializable[0]);
    }

    /**
     * Connection方法远程调用, 带参数
     *
     * @param method 方法名
     * @param params 方法参数
     * @return 可序列化返回值
     * @throws SQLException
     */
    private Serializable invokeConnMethod(Method method, Serializable... params)
            throws SQLException {
        synchronized (jdbcAgentConnector) {
            try {
                Packet responsePacket =
                        Packet.parse(jdbcAgentConnector.write(Packet.newBuilder().incrementAndGetId()
                                .setType(PacketType.CONN_METHOD).setBody(ConnectionMsg.newBuilder()
                                        .setId(remoteId).setMethod(method).setParams(params).build())
                                .build()), SerializeUtil.serializeType);
                ConnectionMsg response = (ConnectionMsg) responsePacket.getBody();
                warnings = response.getWarnings();
                return response.getResponse();
            } catch (Exception e) {
                checkConnection(e);
                throw new SQLException(e);
            }
        }
    }

    /**
     * 从异常信息验证连接是否断开
     *
     * @param t 异常信息
     */
    private void checkConnection(Throwable t) {
        String errMsg = t.getMessage();
        if (errMsg != null &&
                (errMsg.contains("Broken pipe") ||
                        errMsg.contains("end of stream when reading header") ||
                        errMsg.contains("dataSource already closed"))) {
            connected = false;
            jdbcAgentConnector.disconnect();
            jdbcAgentConnector.stop();
        }
    }

    /**
     * !! 关闭server和db的connection
     * <p>
     * 由于 client 所建立的 connection 是和 server 的 TCP 连接, 而非和数据库的真实连接
     * 所以在 client 连接池回收 connection 的时候不会直接调用close方法, 即不会断开 tcp 连接
     * 而在 server 端由于 tcp 连接未被关闭, server 和 db 的 connection 也不会被释放
     * 提供 release 方法请求 server 直接释放和 db 的 connection, 而 tcp 连接可以继续被保持
     * client 端使用连接池需要代理 close 方法, 嵌入 release 方法
     *
     * @throws SQLException
     */
    public void release() throws SQLException {
        invokeConnMethod(Method.release);
    }

    @Override
    public Statement createStatement() throws SQLException {
        long statementId = (Long) invokeConnMethod(Method.createStatement);
        return new JdbcStatement(this, jdbcAgentConnector, statementId);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        long statementId =
                (Long) invokeConnMethod(Method.prepareStatement, sql);
        return new JdbcPreparedStatement(this, jdbcAgentConnector, statementId);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        long statementId =
                (Long) invokeConnMethod(Method.prepareCall, sql);
        return new JdbcCallableStatement(this, jdbcAgentConnector, statementId);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return (String) invokeConnMethod(Method.nativeSQL, sql);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        getSerialConnection(Method.setAutoCommit, autoCommit).setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return getSerialConnection(Method.getAutoCommit).getAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
        invokeConnMethod(Method.commit);
    }

    @Override
    public void rollback() throws SQLException {
        invokeConnMethod(Method.rollback);
    }

    @Override
    public void close() throws SQLException {
        synchronized (jdbcAgentConnector) {
            try {
                serialConnection = null;
                if (connected) {
                    Packet packet = Packet.newBuilder()
                            .incrementAndGetId()
                            .setType(PacketType.CONN_CLOSE)
                            .setBody(ConnectionMsg.newBuilder().setId(remoteId).build()).build();
                    Packet.parse(jdbcAgentConnector.write(packet), SerializeUtil.serializeType).getAck();
                    connected = false;
                }
            } catch (Exception e) {
                // ignore
            }

            jdbcAgentConnector.disconnect();
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return !connected;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        long metaDataId = (Long) invokeConnMethod(Method.getMetaData);
        return new JdbcDatabaseMetaData(this, jdbcAgentConnector, metaDataId);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        getSerialConnection(Method.setReadOnly, readOnly).setReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return getSerialConnection(Method.isReadOnly).isReadOnly();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        getSerialConnection(Method.setCatalog, catalog).setCatalog(catalog);
    }

    @Override
    public String getCatalog() throws SQLException {
        return getSerialConnection(Method.getCatalog).getCatalog();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        getSerialConnection(Method.setTransactionIsolation, level).setTransactionIsolation(level);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return getSerialConnection(Method.getTransactionIsolation).getTransactionIsolation();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        if (warnings != null) {
            return new SQLWarning(warnings);
        } else {
            return null;
        }
    }

    @Override
    public void clearWarnings() throws SQLException {
        // invokeConnMethod(Method.clearWarnings);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws SQLException {
        long statementId = (Long) invokeConnMethod(Method.createStatement, resultSetType, resultSetConcurrency);
        return new JdbcStatement(this, jdbcAgentConnector, statementId);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency) throws SQLException {
        long statementId = (Long) invokeConnMethod(Method.prepareStatement, sql, resultSetType, resultSetConcurrency);
        return new JdbcPreparedStatement(this, jdbcAgentConnector, statementId);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        long statementId =
                (Long) invokeConnMethod(Method.prepareCall, sql, resultSetType, resultSetConcurrency);
        return new JdbcCallableStatement(this, jdbcAgentConnector, statementId);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        getSerialConnection(Method.setHoldability, holdability).setHoldability(holdability);
    }

    @Override
    public int getHoldability() throws SQLException {
        return getSerialConnection(Method.getHoldability).getHoldability();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return (Savepoint) invokeConnMethod(Method.setSavepoint);
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return (Savepoint) invokeConnMethod(Method.setSavepoint, name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        invokeConnMethod(Method.rollback, new SerialSavepoint(savepoint.getSavepointId(), savepoint.getSavepointName()));
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        invokeConnMethod(Method.releaseSavepoint, new SerialSavepoint(savepoint.getSavepointId(), savepoint.getSavepointName()));
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency,
                                     int resultSetHoldability) throws SQLException {
        long statementId = (Long) invokeConnMethod(Method.createStatement, resultSetType, resultSetConcurrency, resultSetHoldability);
        return new JdbcStatement(this, jdbcAgentConnector, statementId);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        long statementId = (Long) invokeConnMethod(Method.prepareStatement, sql,
                resultSetType, resultSetConcurrency, resultSetHoldability);
        return new JdbcPreparedStatement(this, jdbcAgentConnector, statementId);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        long statementId =
                (Long) invokeConnMethod(Method.prepareCall, sql, resultSetType,
                        resultSetConcurrency, resultSetHoldability);
        return new JdbcCallableStatement(this, jdbcAgentConnector, statementId);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        long statementId = (Long) invokeConnMethod(Method.prepareStatement, sql, autoGeneratedKeys);
        return new JdbcPreparedStatement(this, jdbcAgentConnector, statementId);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        long statementId = (Long) invokeConnMethod(Method.prepareStatement, sql, columnIndexes);
        return new JdbcPreparedStatement(this, jdbcAgentConnector, statementId);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames)
            throws SQLException {
        long statementId = (Long) invokeConnMethod(Method.prepareStatement, sql, columnNames);
        return new JdbcPreparedStatement(this, jdbcAgentConnector, statementId);
    }

    @Override
    public Clob createClob() throws SQLException {
        return (Clob) invokeConnMethod(Method.createClob);
    }

    @Override
    public Blob createBlob() throws SQLException {
        return (Blob) invokeConnMethod(Method.createBlob);
    }

    @Override
    public NClob createNClob() throws SQLException {
        return (NClob) invokeConnMethod(Method.createBlob);
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return (boolean) invokeConnMethod(Method.isValid, timeout);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        try {
            getSerialConnection(Method.setClientInfo, name, value).setClientInfo(name, value);
        } catch (SQLException e) {
            // ignore
        }
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        try {
            getSerialConnection(Method.setClientInfo, properties).setClientInfo(properties);
        } catch (SQLException e) {
            // ignore
        }
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return getSerialConnection(Method.getClientInfo).getClientInfo(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return getSerialConnection(Method.getClientInfo).getClientInfo();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return (Array) invokeConnMethod(Method.createArrayOf, typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        getSerialConnection(Method.setSchema, schema).setSchema(schema);
    }

    @Override
    public String getSchema() throws SQLException {
        return getSerialConnection(Method.getSchema).getSchema();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return (Integer) invokeConnMethod(Method.getNetworkTimeout);
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
