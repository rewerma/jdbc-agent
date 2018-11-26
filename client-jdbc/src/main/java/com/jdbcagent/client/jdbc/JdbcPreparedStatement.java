package com.jdbcagent.client.jdbc;

import com.jdbcagent.client.util.SerializeUtil;
import com.jdbcagent.core.protocol.Packet;
import com.jdbcagent.core.protocol.Packet.PacketType;
import com.jdbcagent.core.protocol.PreparedStatementMsg;
import com.jdbcagent.core.protocol.PreparedStatementMsg.Method;
import com.jdbcagent.core.protocol.PreparedStatementMsg.ParamType;
import com.jdbcagent.core.support.serial.SerialNClob;
import com.jdbcagent.core.support.serial.SerialRowId;
import com.jdbcagent.core.util.Util;

import javax.sql.rowset.serial.SerialArray;
import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import javax.sql.rowset.serial.SerialRef;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.Serializable;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.LinkedList;

/**
 * JDBC-Agent client jdbc preparedStatement impl
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class JdbcPreparedStatement extends JdbcStatement implements PreparedStatement {

    private final JdbcAgentConnector jdbcAgentConnector;                            // tcp连接器

    protected LinkedList<PreparedStatementMsg> paramsQueue = new LinkedList<>();    // 参数队列

    /**
     * 构造方法
     *
     * @param conn               connection
     * @param jdbcAgentConnector tcp连接器
     * @param remoteId           远程preparedStatementId
     */
    JdbcPreparedStatement(Connection conn, JdbcAgentConnector jdbcAgentConnector, long remoteId) {
        super(conn, jdbcAgentConnector, remoteId);
        this.jdbcAgentConnector = jdbcAgentConnector;
    }

    /**
     * PreparedStatement方法远程调用, 带参数
     *
     * @param method 方法名
     * @param params 方法参数
     * @return 可序列化返回值
     * @throws SQLException
     */
    private Serializable invokePreparedStatementMethod(Method method, Serializable... params)
            throws SQLException {
        synchronized (jdbcAgentConnector) {
            Packet responsePacket = Packet.parse(
                    jdbcAgentConnector.write(Packet.newBuilder()
                            .incrementAndGetId()
                            .setType(PacketType.PRE_STMT_METHOD)
                            .setBody(PreparedStatementMsg.newBuilder().setId(remoteId)
                                    .setMethod(method).setParams(params).build())
                            .build()), SerializeUtil.serializeType);
            return ((PreparedStatementMsg) responsePacket.getBody()).getResponse();
        }
    }

    /**
     * 设置参数队列
     *
     * @param paramType      参数类型
     * @param parameterIndex 参数索引值
     * @param param          参数值
     */
    protected void setParam(ParamType paramType, int parameterIndex, Serializable... param) {
        PreparedStatementMsg.Builder builder = PreparedStatementMsg.newBuilder().setId(remoteId)
                .setParamType(paramType).setParameterIndex(parameterIndex);

        PreparedStatementMsg preparedStatementMsg = builder.setParams(param).build();

        paramsQueue.offer(preparedStatementMsg);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        Long resultSetId = (Long) invokePreparedStatementMethod(Method.executeQuery, paramsQueue);
        return new JdbcResultSet(jdbcAgentConnector, resultSetId);
    }

    @Override
    public int executeUpdate() throws SQLException {
        return (Integer) invokePreparedStatementMethod(Method.executeUpdate, paramsQueue);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setParam(ParamType.NULL, parameterIndex, sqlType);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setParam(ParamType.BOOLEAN, parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setParam(ParamType.BYTE, parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setParam(ParamType.SHORT, parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        setParam(ParamType.INT, parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        setParam(ParamType.LONG, parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setParam(ParamType.FLOAT, parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        setParam(ParamType.DOUBLE, parameterIndex, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        setParam(ParamType.BIG_DECIMAL, parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        setParam(ParamType.STRING, parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        setParam(ParamType.BYTES, parameterIndex, (Serializable) x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        setParam(ParamType.DATE, parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        setParam(ParamType.TIME, parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setParam(ParamType.TIMESTAMP, parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        try {
            setParam(ParamType.ASCII_STREAM, parameterIndex, Util.input2Bytes(x, length), length);
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length)
            throws SQLException {
        try {
            setParam(ParamType.UNICODE_STREAM, parameterIndex, Util.input2Bytes(x, length), length);
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        try {
            setParam(ParamType.BINARY_STREAM, parameterIndex, Util.input2Bytes(x, length), length);
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void clearParameters() throws SQLException {
        invokePreparedStatementMethod(Method.clearParameters);
        paramsQueue.clear();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setParam(ParamType.OBJECT, parameterIndex, (Serializable) x, targetSqlType);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        setParam(ParamType.OBJECT, parameterIndex, (Serializable) x);
    }

    @Override
    public boolean execute() throws SQLException {
        return (Boolean) invokePreparedStatementMethod(Method.clearParameters, paramsQueue);
    }

    @Override
    public void addBatch() throws SQLException {
        invokePreparedStatementMethod(Method.addBatch);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length)
            throws SQLException {
        try {
            setParam(ParamType.CHARACTER_STREAM, parameterIndex, Util.reader2String(reader, length),
                    length);
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        setParam(ParamType.REF, parameterIndex, new SerialRef(x));
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        setParam(ParamType.BLOB, parameterIndex, new SerialBlob(x));
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        setParam(ParamType.CLOB, parameterIndex, new SerialClob(x));
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        setParam(ParamType.ARRAY, parameterIndex, new SerialArray(x));
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        setParam(ParamType.DATE, parameterIndex, x, cal);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        setParam(ParamType.TIME, parameterIndex, x, cal);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        setParam(ParamType.TIMESTAMP, parameterIndex, x, cal);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setParam(ParamType.NULL, parameterIndex, sqlType, typeName);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        setParam(ParamType.URL, parameterIndex, x);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        setParam(ParamType.ROW_ID, parameterIndex, new SerialRowId(x.getBytes()));
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setParam(ParamType.NSTRING, parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length)
            throws SQLException {
        try {
            setParam(ParamType.NCHARACTER_STREAM, parameterIndex,
                    Util.reader2String(value, (int) length), length);
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        setParam(ParamType.NCLOB, parameterIndex, new SerialNClob(value));
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        try {
            setParam(ParamType.CLOB, parameterIndex, new SerialClob(Util.reader2Chars(reader, (int) length)));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length)
            throws SQLException {
        try {
            setParam(ParamType.BLOB, parameterIndex,
                    new SerialBlob(Util.input2Bytes(inputStream, (int) length)));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        try {
            setParam(ParamType.NCLOB, parameterIndex, new SerialNClob(Util.reader2Chars(reader, (int) length)));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
            throws SQLException {
        setParam(ParamType.OBJECT, parameterIndex, (Serializable) x, targetSqlType, scaleOrLength);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        try {
            setParam(ParamType.ASCII_STREAM, parameterIndex,
                    (Serializable) Util.input2Bytes(x, (int) length));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length)
            throws SQLException {
        try {
            setParam(ParamType.BINARY_STREAM, parameterIndex,
                    (Serializable) Util.input2Bytes(x, (int) length));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length)
            throws SQLException {
        try {
            setParam(ParamType.CHARACTER_STREAM, parameterIndex,
                    Util.reader2String(reader, (int) length));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        try {
            setParam(ParamType.ASCII_STREAM, parameterIndex, (Serializable) Util.input2Bytes(x));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        try {
            setParam(ParamType.BINARY_STREAM, parameterIndex, (Serializable) Util.input2Bytes(x));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        try {
            setParam(ParamType.CHARACTER_STREAM, parameterIndex, Util.reader2String(reader));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        try {
            setParam(ParamType.NCHARACTER_STREAM, parameterIndex, Util.reader2String(value));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        try {
            setParam(ParamType.CLOB, parameterIndex, new SerialNClob(Util.reader2Chars(reader)));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        try {
            setParam(ParamType.BLOB, parameterIndex, new SerialBlob(Util.input2Bytes(inputStream)));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        try {
            setParam(ParamType.NCLOB, parameterIndex, new SerialNClob(Util.reader2Chars(reader)));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void close() throws SQLException {
        synchronized (jdbcAgentConnector) {
            paramsQueue.clear(); // 清除参数队列
            Packet packet = Packet.newBuilder()
                    .incrementAndGetId()
                    .setType(PacketType.PRE_STMT_CLOSE)
                    .setBody(PreparedStatementMsg.newBuilder().setId(remoteId).build()).build();
            Packet.parse(jdbcAgentConnector.write(packet), SerializeUtil.serializeType).getAck();
        }
    }

}
