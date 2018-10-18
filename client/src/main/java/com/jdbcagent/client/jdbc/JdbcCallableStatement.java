package com.jdbcagent.client.jdbc;

import com.jdbcagent.client.JdbcAgentConnector;
import com.jdbcagent.client.uitl.SerializeUtil;
import com.jdbcagent.core.protocol.CallableStatementMsg;
import com.jdbcagent.core.protocol.CallableStatementMsg.Method;
import com.jdbcagent.core.protocol.Packet;
import com.jdbcagent.core.protocol.Packet.PacketType;
import com.jdbcagent.core.protocol.PreparedStatementMsg.ParamType;
import com.jdbcagent.core.support.serial.SerialNClob;
import com.jdbcagent.core.support.serial.SerialRowId;
import com.jdbcagent.core.util.Util;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.Serializable;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * JDBC-Agent client jdbc callableStatement impl
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class JdbcCallableStatement extends JdbcPreparedStatement implements CallableStatement {

    private final JdbcAgentConnector jdbcAgentConnector;                             // tcp连接器

    private LinkedList<CallableStatementMsg> csParamsQueue = new LinkedList<>();     // 参数队列

    /**
     * 构造方法
     *
     * @param conn               connection
     * @param jdbcAgentConnector tcp连接器
     * @param remoteId           远程callableStatementId
     */
    JdbcCallableStatement(Connection conn, JdbcAgentConnector jdbcAgentConnector, long remoteId) {
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
    private Serializable invokeCallableStatementMethod(Method method, Serializable... params)
            throws SQLException {
        synchronized (jdbcAgentConnector) {
            Packet responsePacket = Packet.parse(
                    jdbcAgentConnector.write(Packet.newBuilder()
                            .incrementAndGetId()
                            .setType(PacketType.CLA_STMT_METHOD)
                            .setBody(CallableStatementMsg.newBuilder().setId(remoteId)
                                    .setMethod(method).setParams(params).build())
                            .build()), SerializeUtil.serializeType);
            return ((CallableStatementMsg) responsePacket.getBody()).getResponse();
        }
    }

    /**
     * 设置参数队列
     *
     * @param paramType     参数类型
     * @param parameterName 参数名
     * @param param         参数值
     */
    private void setParam(ParamType paramType, String parameterName, Serializable... param) {
        CallableStatementMsg.Builder builder = CallableStatementMsg.newBuilder().setId(remoteId)
                .setParamType(paramType).setParameterName(parameterName);

        CallableStatementMsg callableStatementMsg = builder.setParams(param).build();

        csParamsQueue.offer(callableStatementMsg);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        Long resultSetId = (Long) invokeCallableStatementMethod(Method.executeQuery, paramsQueue, csParamsQueue);
        return new JdbcResultSet(jdbcAgentConnector, resultSetId);
    }

    @Override
    public boolean execute() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.execute, paramsQueue, csParamsQueue);
    }

    @Override
    public int executeUpdate() throws SQLException {
        return (Integer) invokeCallableStatementMethod(Method.executeUpdate, paramsQueue, csParamsQueue);
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        setParam(ParamType.registerOutParameter, parameterIndex, sqlType);
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
        setParam(ParamType.registerOutParameter, parameterIndex, sqlType, scale);
    }

    @Override
    public boolean wasNull() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.wasNull);
    }

    @Override
    public String getString(int parameterIndex) throws SQLException {
        return (String) invokeCallableStatementMethod(Method.getString, parameterIndex);
    }

    @Override
    public boolean getBoolean(int parameterIndex) throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.getBoolean, parameterIndex);
    }

    @Override
    public byte getByte(int parameterIndex) throws SQLException {
        return (Byte) invokeCallableStatementMethod(Method.getByte, parameterIndex);
    }

    @Override
    public short getShort(int parameterIndex) throws SQLException {
        return (Short) invokeCallableStatementMethod(Method.getShort, parameterIndex);
    }

    @Override
    public int getInt(int parameterIndex) throws SQLException {
        return (Integer) invokeCallableStatementMethod(Method.getInt, parameterIndex);
    }

    @Override
    public long getLong(int parameterIndex) throws SQLException {
        return (Long) invokeCallableStatementMethod(Method.getLong, parameterIndex);
    }

    @Override
    public float getFloat(int parameterIndex) throws SQLException {
        return (Float) invokeCallableStatementMethod(Method.getFloat, parameterIndex);
    }

    @Override
    public double getDouble(int parameterIndex) throws SQLException {
        return (Double) invokeCallableStatementMethod(Method.getDouble, parameterIndex);
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        return (BigDecimal) invokeCallableStatementMethod(Method.getBigDecimal, parameterIndex, scale);
    }

    @Override
    public byte[] getBytes(int parameterIndex) throws SQLException {
        return (byte[]) invokeCallableStatementMethod(Method.getBytes, parameterIndex);
    }

    @Override
    public Date getDate(int parameterIndex) throws SQLException {
        return (Date) invokeCallableStatementMethod(Method.getDate, parameterIndex);
    }

    @Override
    public Time getTime(int parameterIndex) throws SQLException {
        return (Time) invokeCallableStatementMethod(Method.getTime, parameterIndex);
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        return (Timestamp) invokeCallableStatementMethod(Method.getTimestamp, parameterIndex);
    }

    @Override
    public Object getObject(int parameterIndex) throws SQLException {
        return invokeCallableStatementMethod(Method.getObject, parameterIndex);
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        return (BigDecimal) invokeCallableStatementMethod(Method.getBigDecimal, parameterIndex);
    }

    @Override
    public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
        if (map != null) {
            return invokeCallableStatementMethod(Method.getObject, parameterIndex, new LinkedHashMap<>(map));
        }
        return null;
    }

    @Override
    public Ref getRef(int parameterIndex) throws SQLException {
        return (Ref) invokeCallableStatementMethod(Method.getRef, parameterIndex);
    }

    @Override
    public Blob getBlob(int parameterIndex) throws SQLException {
        return (Blob) invokeCallableStatementMethod(Method.getBlob, parameterIndex);
    }

    @Override
    public Clob getClob(int parameterIndex) throws SQLException {
        return (Clob) invokeCallableStatementMethod(Method.getClob, parameterIndex);
    }

    @Override
    public Array getArray(int parameterIndex) throws SQLException {
        return (Array) invokeCallableStatementMethod(Method.getArray, parameterIndex);
    }

    @Override
    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        return (Date) invokeCallableStatementMethod(Method.getDate, parameterIndex);
    }

    @Override
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        return (Time) invokeCallableStatementMethod(Method.getTime, parameterIndex);
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        return (Timestamp) invokeCallableStatementMethod(Method.getTimestamp, parameterIndex);
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setParam(ParamType.registerOutParameter, parameterIndex, sqlType, typeName);
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
        setParam(ParamType.registerOutParameter, parameterName, sqlType);
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
        setParam(ParamType.registerOutParameter, parameterName, sqlType, scale);
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
        setParam(ParamType.registerOutParameter, parameterName, sqlType, typeName);
    }

    @Override
    public URL getURL(int parameterIndex) throws SQLException {
        return null;
    }

    @Override
    public void setURL(String parameterName, URL val) throws SQLException {
        setParam(ParamType.URL, parameterName, val);
    }

    @Override
    public void setNull(String parameterName, int sqlType) throws SQLException {
        setParam(ParamType.NULL, parameterName, sqlType);
    }

    @Override
    public void setBoolean(String parameterName, boolean x) throws SQLException {
        setParam(ParamType.BOOLEAN, parameterName, x);
    }

    @Override
    public void setByte(String parameterName, byte x) throws SQLException {
        setParam(ParamType.BYTE, parameterName, x);
    }

    @Override
    public void setShort(String parameterName, short x) throws SQLException {
        setParam(ParamType.SHORT, parameterName, x);
    }

    @Override
    public void setInt(String parameterName, int x) throws SQLException {
        setParam(ParamType.INT, parameterName, x);
    }

    @Override
    public void setLong(String parameterName, long x) throws SQLException {
        setParam(ParamType.LONG, parameterName, x);
    }

    @Override
    public void setFloat(String parameterName, float x) throws SQLException {
        setParam(ParamType.FLOAT, parameterName, x);
    }

    @Override
    public void setDouble(String parameterName, double x) throws SQLException {
        setParam(ParamType.DOUBLE, parameterName, x);
    }

    @Override
    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
        setParam(ParamType.BIG_DECIMAL, parameterName, x);
    }

    @Override
    public void setString(String parameterName, String x) throws SQLException {
        setParam(ParamType.STRING, parameterName, x);
    }

    @Override
    public void setBytes(String parameterName, byte[] x) throws SQLException {
        setParam(ParamType.BYTES, parameterName, x);
    }

    @Override
    public void setDate(String parameterName, Date x) throws SQLException {
        setParam(ParamType.DATE, parameterName, x);
    }

    @Override
    public void setTime(String parameterName, Time x) throws SQLException {
        setParam(ParamType.TIME, parameterName, x);
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
        setParam(ParamType.TIMESTAMP, parameterName, x);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
        try {
            setParam(ParamType.ASCII_STREAM, parameterName, Util.input2Bytes(x, length), length);
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
        try {
            setParam(ParamType.BINARY_STREAM, parameterName, Util.input2Bytes(x, length), length);
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
        setParam(ParamType.OBJECT, parameterName, (Serializable) x, targetSqlType, scale);
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
        setParam(ParamType.OBJECT, parameterName, (Serializable) x, targetSqlType);
    }

    @Override
    public void setObject(String parameterName, Object x) throws SQLException {
        setParam(ParamType.OBJECT, parameterName, (Serializable) x);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
        try {
            setParam(ParamType.CHARACTER_STREAM, parameterName, Util.reader2String(reader, length), length);
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
        setParam(ParamType.DATE, parameterName, x, cal);
    }

    @Override
    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
        setParam(ParamType.TIME, parameterName, x, cal);
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
        setParam(ParamType.TIMESTAMP, parameterName, x, cal);
    }

    @Override
    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        setParam(ParamType.NULL, parameterName, sqlType, typeName);
    }

    @Override
    public String getString(String parameterName) throws SQLException {
        return (String) invokeCallableStatementMethod(Method.getString, parameterName);
    }

    @Override
    public boolean getBoolean(String parameterName) throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.getBoolean, parameterName);
    }

    @Override
    public byte getByte(String parameterName) throws SQLException {
        return (Byte) invokeCallableStatementMethod(Method.getByte, parameterName);
    }

    @Override
    public short getShort(String parameterName) throws SQLException {
        return (Short) invokeCallableStatementMethod(Method.getShort, parameterName);
    }

    @Override
    public int getInt(String parameterName) throws SQLException {
        return (Integer) invokeCallableStatementMethod(Method.getInt, parameterName);
    }

    @Override
    public long getLong(String parameterName) throws SQLException {
        return (Long) invokeCallableStatementMethod(Method.getLong, parameterName);
    }

    @Override
    public float getFloat(String parameterName) throws SQLException {
        return (Float) invokeCallableStatementMethod(Method.getFloat, parameterName);
    }

    @Override
    public double getDouble(String parameterName) throws SQLException {
        return (Double) invokeCallableStatementMethod(Method.getDouble, parameterName);
    }

    @Override
    public byte[] getBytes(String parameterName) throws SQLException {
        return (byte[]) invokeCallableStatementMethod(Method.getBytes, parameterName);
    }

    @Override
    public Date getDate(String parameterName) throws SQLException {
        return (Date) invokeCallableStatementMethod(Method.getDate, parameterName);
    }

    @Override
    public Time getTime(String parameterName) throws SQLException {
        return (Time) invokeCallableStatementMethod(Method.getTime, parameterName);
    }

    @Override
    public Timestamp getTimestamp(String parameterName) throws SQLException {
        return (Timestamp) invokeCallableStatementMethod(Method.getTimestamp, parameterName);
    }

    @Override
    public Object getObject(String parameterName) throws SQLException {
        return invokeCallableStatementMethod(Method.getObject, parameterName);
    }

    @Override
    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        return (BigDecimal) invokeCallableStatementMethod(Method.getBigDecimal, parameterName);
    }

    @Override
    public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
        if (map != null) {
            return invokeCallableStatementMethod(Method.getObject, parameterName, new LinkedHashMap<>(map));
        }
        return null;
    }

    @Override
    public Ref getRef(String parameterName) throws SQLException {
        return (Ref) invokeCallableStatementMethod(Method.getRef, parameterName);
    }

    @Override
    public Blob getBlob(String parameterName) throws SQLException {
        return (Blob) invokeCallableStatementMethod(Method.getBlob, parameterName);
    }

    @Override
    public Clob getClob(String parameterName) throws SQLException {
        return (Clob) invokeCallableStatementMethod(Method.getClob, parameterName);
    }

    @Override
    public Array getArray(String parameterName) throws SQLException {
        return (Array) invokeCallableStatementMethod(Method.getArray, parameterName);
    }

    @Override
    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        return (Date) invokeCallableStatementMethod(Method.getDate, parameterName);
    }

    @Override
    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        return (Time) invokeCallableStatementMethod(Method.getTime, parameterName);
    }

    @Override
    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        return (Timestamp) invokeCallableStatementMethod(Method.getTimestamp, parameterName);
    }

    @Override
    public URL getURL(String parameterName) throws SQLException {
        return (URL) invokeCallableStatementMethod(Method.getURL, parameterName);
    }

    @Override
    public RowId getRowId(int parameterIndex) throws SQLException {
        return (RowId) invokeCallableStatementMethod(Method.getRowId, parameterIndex);
    }

    @Override
    public RowId getRowId(String parameterName) throws SQLException {
        return (RowId) invokeCallableStatementMethod(Method.getRowId, parameterName);
    }

    @Override
    public void setRowId(String parameterName, RowId x) throws SQLException {
        setParam(ParamType.ROW_ID, parameterName, new SerialRowId(x.getBytes()));
    }

    @Override
    public void setNString(String parameterName, String value) throws SQLException {
        setParam(ParamType.NSTRING, parameterName, value);
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
        try {
            setParam(ParamType.NCHARACTER_STREAM, parameterName, Util.reader2String(value, (int) length), length);
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setNClob(String parameterName, NClob value) throws SQLException {
        setParam(ParamType.NCLOB, parameterName, new SerialNClob(value));
    }

    @Override
    public void setClob(String parameterName, Reader reader, long length) throws SQLException {
        try {
            setParam(ParamType.CLOB, parameterName, new SerialClob(Util.reader2Chars(reader, (int) length)));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
        try {
            setParam(ParamType.BLOB, parameterName, new SerialBlob(Util.input2Bytes(inputStream, (int) length)));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
        try {
            setParam(ParamType.NCLOB, parameterName, new SerialNClob(Util.reader2Chars(reader, (int) length)));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public NClob getNClob(int parameterIndex) throws SQLException {
        return (NClob) invokeCallableStatementMethod(Method.getNClob, parameterIndex);
    }

    @Override
    public NClob getNClob(String parameterName) throws SQLException {
        return (NClob) invokeCallableStatementMethod(Method.getNClob, parameterName);
    }

    @Override
    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SQLXML getSQLXML(String parameterName) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getNString(int parameterIndex) throws SQLException {
        return (String) invokeCallableStatementMethod(Method.getNString, parameterIndex);
    }

    @Override
    public String getNString(String parameterName) throws SQLException {
        return (String) invokeCallableStatementMethod(Method.getNString, parameterName);
    }

    @Override
    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        String res = (String) invokeCallableStatementMethod(Method.getNCharacterStream, parameterIndex);
        if (res != null) {
            return Util.string2Reader(res);
        }
        return null;
    }

    @Override
    public Reader getNCharacterStream(String parameterName) throws SQLException {
        String res = (String) invokeCallableStatementMethod(Method.getNCharacterStream, parameterName);
        if (res != null) {
            return Util.string2Reader(res);
        }
        return null;
    }

    @Override
    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        String res = (String) invokeCallableStatementMethod(Method.getNCharacterStream, parameterIndex);
        if (res != null) {
            return Util.string2Reader(res);
        }
        return null;
    }

    @Override
    public Reader getCharacterStream(String parameterName) throws SQLException {
        String res = (String) invokeCallableStatementMethod(Method.getCharacterStream, parameterName);
        if (res != null) {
            return Util.string2Reader(res);
        }
        return null;
    }

    @Override
    public void setBlob(String parameterName, Blob x) throws SQLException {
        setParam(ParamType.BLOB, parameterName, new SerialBlob(x));
    }

    @Override
    public void setClob(String parameterName, Clob x) throws SQLException {
        setParam(ParamType.CLOB, parameterName, new SerialClob(x));
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
        try {
            setParam(ParamType.ASCII_STREAM, parameterName, (Serializable) Util.input2Bytes(x, (int) length));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
        try {
            setParam(ParamType.BINARY_STREAM, parameterName, (Serializable) Util.input2Bytes(x, (int) length));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
        try {
            setParam(ParamType.CHARACTER_STREAM, parameterName, Util.reader2String(reader, (int) length));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
        try {
            setParam(ParamType.ASCII_STREAM, parameterName, (Serializable) Util.input2Bytes(x));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
        try {
            setParam(ParamType.BINARY_STREAM, parameterName, (Serializable) Util.input2Bytes(x));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
        try {
            setParam(ParamType.CHARACTER_STREAM, parameterName, Util.reader2String(reader));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
        try {
            setParam(ParamType.NCHARACTER_STREAM, parameterName, Util.reader2String(value));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setClob(String parameterName, Reader reader) throws SQLException {
        try {
            setParam(ParamType.CLOB, parameterName, new SerialClob(Util.reader2Chars(reader)));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
        try {
            setParam(ParamType.BLOB, parameterName, new SerialBlob(Util.input2Bytes(inputStream)));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setNClob(String parameterName, Reader reader) throws SQLException {
        try {
            setParam(ParamType.NCLOB, parameterName, new SerialNClob(Util.reader2Chars(reader)));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
        return (T) invokeCallableStatementMethod(Method.getObject, parameterIndex);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
        return (T) invokeCallableStatementMethod(Method.getObject, parameterName);
    }

    @Override
    public void close() throws SQLException {
        synchronized (jdbcAgentConnector) {
            paramsQueue.clear();
            csParamsQueue.clear(); // 清除参数队列
            Packet packet = Packet.newBuilder()
                    .incrementAndGetId()
                    .setType(PacketType.CLA_STMT_CLOSE)
                    .setBody(CallableStatementMsg.newBuilder()
                            .setId(remoteId).build()).build();
            Packet.parse(jdbcAgentConnector.write(packet), SerializeUtil.serializeType).getAck();
        }
    }
}
