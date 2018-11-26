package com.jdbcagent.client.jdbc;

import com.jdbcagent.client.util.SerializeUtil;
import com.jdbcagent.core.protocol.Packet;
import com.jdbcagent.core.protocol.Packet.PacketType;
import com.jdbcagent.core.protocol.ResultSetMsg;

import javax.sql.RowSet;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

/**
 * JDBC-Agent client jdbc resultSet impl
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class JdbcResultSet implements ResultSet {
    private final static int BATCH_SIZE = 500;              // 每次批量获取的行数

    private final JdbcAgentConnector jdbcAgentConnector;    // tcp连接器

    private long remoteId;                                  // 远程resultSetId

    private int offset = 1;                                 // 当前偏移量

    private RowSet rowSet;                                  // 可序列化的resultSet

    /**
     * ResutSet构造方法
     *
     * @param jdbcAgentConnector tcp连接器
     * @param remoteId           远程resultSetId
     * @throws SQLException
     */
    JdbcResultSet(JdbcAgentConnector jdbcAgentConnector, long remoteId) throws SQLException {
        this.remoteId = remoteId;
        this.jdbcAgentConnector = jdbcAgentConnector;
    }

    /**
     * 远程调用获取resultSet的元数据并转换为可序列化rs返回
     *
     * @throws SQLException
     */
    private void invokeMetaData() throws SQLException {
        synchronized (jdbcAgentConnector) {
            Packet packet = Packet.newBuilder()
                    .incrementAndGetId()
                    .setType(PacketType.RS_META_DATA)
                    .setBody(ResultSetMsg.newBuilder()
                            .setId(remoteId).build())
                    .build();
            Packet responsePacket = Packet.parse(jdbcAgentConnector.write(packet), SerializeUtil.serializeType);
            ResultSetMsg resultSetMsg = (ResultSetMsg) responsePacket.getBody();
            clearRowSet();
            rowSet = resultSetMsg.getRowSet();
        }
    }

    /**
     * 远程调用批量获取指定行数的resultSet并转换为可序列化rs返回
     *
     * @throws SQLException
     */
    private void fetchRows() throws SQLException {
        synchronized (jdbcAgentConnector) {
            Packet packet = Packet.newBuilder()
                    .incrementAndGetId()
                    .setType(PacketType.RS_FETCH_ROWS)
                    .setBody(ResultSetMsg.newBuilder()
                            .setId(remoteId).setBatchSize(BATCH_SIZE).build())
                    .build();
            Packet responsePacket = Packet.parse(jdbcAgentConnector.write(packet), SerializeUtil.serializeType);
            ResultSetMsg resultSetMsg = (ResultSetMsg) responsePacket.getBody();
            clearRowSet();
            rowSet = resultSetMsg.getRowSet();
        }
    }

    /**
     * 清除可序列化结果集
     *
     * @throws SQLException
     */
    private void clearRowSet() throws SQLException {
        if (rowSet != null) {
            rowSet.close();
            rowSet = null;
        }
    }

    private RowSet getRowSet() {
        return rowSet;
    }

    /**
     * 批量获取数据
     *
     * @return
     * @throws SQLException
     */
    @Override
    public boolean next() throws SQLException {
        if (offset == 1) {
            fetchRows();
            offset++;
            return rowSet.next();
        } else {
            if (rowSet.next()) {
                offset++;
                return true;
            } else {
                if ((offset - 1) % BATCH_SIZE == 0) {
                    fetchRows();
                    offset++;
                    return rowSet.next();
                } else {
                    return false;
                }
            }
        }
    }

    @Override
    public void close() throws SQLException {
        synchronized (jdbcAgentConnector) {
            Packet packet = Packet.newBuilder()
                    .incrementAndGetId()
                    .setType(PacketType.RS_CLOSE)
                    .setBody(ResultSetMsg.newBuilder().setId(remoteId).build()).build();
            Packet responsePacket = Packet.parse(jdbcAgentConnector.write(packet), SerializeUtil.serializeType);
            responsePacket.getAck();
        }
    }

    @Override
    public boolean wasNull() throws SQLException {
        return getRowSet().wasNull();
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return getRowSet().getString(columnIndex);

    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return getRowSet().getBoolean(columnIndex);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return getRowSet().getByte(columnIndex);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return getRowSet().getShort(columnIndex);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return getRowSet().getInt(columnIndex);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return getRowSet().getLong(columnIndex);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return getRowSet().getFloat(columnIndex);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return getRowSet().getFloat(columnIndex);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return getRowSet().getBigDecimal(columnIndex, scale);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return getRowSet().getBytes(columnIndex);
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return getRowSet().getDate(columnIndex);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return getRowSet().getTime(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return getRowSet().getTimestamp(columnIndex);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return getRowSet().getAsciiStream(columnIndex);
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return getRowSet().getUnicodeStream(columnIndex);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return getRowSet().getBinaryStream(columnIndex);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getRowSet().getString(columnLabel);
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getRowSet().getBoolean(columnLabel);
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getRowSet().getByte(columnLabel);
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getRowSet().getShort(columnLabel);
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getRowSet().getInt(columnLabel);
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getRowSet().getLong(columnLabel);
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getRowSet().getFloat(columnLabel);
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getRowSet().getDouble(columnLabel);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getRowSet().getBigDecimal(columnLabel, scale);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getRowSet().getBytes(columnLabel);
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getRowSet().getDate(columnLabel);
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getRowSet().getTime(columnLabel);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getRowSet().getTimestamp(columnLabel);
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getRowSet().getAsciiStream(columnLabel);
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getRowSet().getUnicodeStream(columnLabel);
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getRowSet().getBinaryStream(columnLabel);
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return getRowSet().getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        getRowSet().clearWarnings();
    }

    @Override
    public String getCursorName() throws SQLException {
        return getRowSet().getCursorName();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        invokeMetaData();
        return getRowSet().getMetaData();
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getRowSet().getObject(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getRowSet().getObject(columnLabel);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return getRowSet().findColumn(columnLabel);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        return getRowSet().getCharacterStream(columnIndex);
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return getRowSet().getCharacterStream(columnLabel);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return getRowSet().getBigDecimal(columnIndex);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getRowSet().getBigDecimal(columnLabel);
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return getRowSet().isBeforeFirst();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return getRowSet().isAfterLast();
    }

    @Override
    public boolean isFirst() throws SQLException {
        return getRowSet().isFirst();
    }

    @Override
    public boolean isLast() throws SQLException {
        return getRowSet().isLast();
    }

    @Override
    public void beforeFirst() throws SQLException {
        getRowSet().beforeFirst();
    }

    @Override
    public void afterLast() throws SQLException {
        getRowSet().afterLast();
    }

    @Override
    public boolean first() throws SQLException {
        return getRowSet().first();
    }

    @Override
    public boolean last() throws SQLException {
        return getRowSet().last();
    }

    @Override
    public int getRow() throws SQLException {
        return getRowSet().getRow();
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        return getRowSet().absolute(row);
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        return getRowSet().relative(rows);
    }

    @Override
    public boolean previous() throws SQLException {
        return getRowSet().previous();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        getRowSet().setFetchDirection(direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return getRowSet().getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        getRowSet().setFetchDirection(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return getRowSet().getFetchSize();
    }

    @Override
    public int getType() throws SQLException {
        return getRowSet().getType();
    }

    @Override
    public int getConcurrency() throws SQLException {
        return getRowSet().getConcurrency();
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        return getRowSet().rowUpdated();
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return getRowSet().rowInserted();
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return getRowSet().rowDeleted();
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        getRowSet().updateNull(columnIndex);
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        getRowSet().updateBoolean(columnIndex, x);
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        getRowSet().updateByte(columnIndex, x);
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        getRowSet().updateShort(columnIndex, x);
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        getRowSet().updateInt(columnIndex, x);
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        getRowSet().updateLong(columnIndex, x);
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        getRowSet().updateFloat(columnIndex, x);
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        getRowSet().updateDouble(columnIndex, x);
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        getRowSet().updateBigDecimal(columnIndex, x);
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        getRowSet().updateString(columnIndex, x);
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        getRowSet().updateBytes(columnIndex, x);
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        getRowSet().updateDate(columnIndex, x);
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        getRowSet().updateTime(columnIndex, x);
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        getRowSet().updateTimestamp(columnIndex, x);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        getRowSet().updateAsciiStream(columnIndex, x);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        getRowSet().updateBinaryStream(columnIndex, x, length);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        getRowSet().updateCharacterStream(columnIndex, x, length);
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        getRowSet().updateObject(columnIndex, x, scaleOrLength);
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        getRowSet().updateObject(columnIndex, x);
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        getRowSet().updateNull(columnLabel);
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        getRowSet().updateBoolean(columnLabel, x);
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        getRowSet().updateByte(columnLabel, x);
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        getRowSet().updateShort(columnLabel, x);
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        getRowSet().updateInt(columnLabel, x);
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        getRowSet().updateLong(columnLabel, x);
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        getRowSet().updateFloat(columnLabel, x);
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        getRowSet().updateDouble(columnLabel, x);
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        getRowSet().updateBigDecimal(columnLabel, x);
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        getRowSet().updateString(columnLabel, x);
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        getRowSet().updateBytes(columnLabel, x);
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        getRowSet().updateDate(columnLabel, x);
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        getRowSet().updateTime(columnLabel, x);
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        getRowSet().updateTimestamp(columnLabel, x);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length)
            throws SQLException {
        getRowSet().updateAsciiStream(columnLabel, x, length);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length)
            throws SQLException {
        getRowSet().updateBinaryStream(columnLabel, x, length);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length)
            throws SQLException {
        getRowSet().updateCharacterStream(columnLabel, reader, length);
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        getRowSet().updateObject(columnLabel, x, scaleOrLength);
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        getRowSet().updateObject(columnLabel, x);
    }

    @Override
    public void insertRow() throws SQLException {
        getRowSet().insertRow();
    }

    @Override
    public void updateRow() throws SQLException {
        getRowSet().updateRow();
    }

    @Override
    public void deleteRow() throws SQLException {
        getRowSet().deleteRow();
    }

    @Override
    public void refreshRow() throws SQLException {
        getRowSet().refreshRow();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        getRowSet().cancelRowUpdates();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        getRowSet().moveToInsertRow();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        getRowSet().moveToCurrentRow();
    }

    @Override
    public Statement getStatement() throws SQLException {
        return getRowSet().getStatement();
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return getRowSet().getObject(columnIndex, map);
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        return getRowSet().getRef(columnIndex);
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        return getRowSet().getBlob(columnIndex);
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        return getRowSet().getClob(columnIndex);
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        return getRowSet().getArray(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getRowSet().getObject(columnLabel, map);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        return getRowSet().getRef(columnLabel);
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return getRowSet().getBlob(columnLabel);
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return getRowSet().getClob(columnLabel);
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return getRowSet().getArray(columnLabel);
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return getRowSet().getDate(columnIndex, cal);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getRowSet().getDate(columnLabel, cal);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return getRowSet().getTime(columnIndex, cal);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getRowSet().getTime(columnLabel, cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return getRowSet().getTimestamp(columnIndex, cal);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getRowSet().getTimestamp(columnLabel, cal);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        return getRowSet().getURL(columnIndex);
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return getRowSet().getURL(columnLabel);
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        getRowSet().updateRef(columnIndex, x);
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        getRowSet().updateRef(columnLabel, x);
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        getRowSet().updateBlob(columnIndex, x);
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        getRowSet().updateBlob(columnLabel, x);
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        getRowSet().updateClob(columnIndex, x);
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        getRowSet().updateClob(columnLabel, x);
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        getRowSet().updateArray(columnIndex, x);
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        getRowSet().updateArray(columnLabel, x);
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        return getRowSet().getRowId(columnIndex);
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return getRowSet().getRowId(columnLabel);
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        getRowSet().updateRowId(columnIndex, x);
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        getRowSet().updateRowId(columnLabel, x);
    }

    @Override
    public int getHoldability() throws SQLException {
        return getRowSet().getHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return getRowSet().isClosed();
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        getRowSet().updateNString(columnIndex, nString);
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        getRowSet().updateNString(columnLabel, nString);
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        getRowSet().updateNClob(columnIndex, nClob);
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        getRowSet().updateNClob(columnLabel, nClob);
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        return getRowSet().getNClob(columnIndex);
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        return getRowSet().getNClob(columnLabel);
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        return getRowSet().getSQLXML(columnIndex);
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return getRowSet().getSQLXML(columnLabel);
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        getRowSet().updateSQLXML(columnIndex, xmlObject);
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        getRowSet().updateSQLXML(columnLabel, xmlObject);
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return getRowSet().getNString(columnIndex);
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getRowSet().getNString(columnLabel);
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return getRowSet().getNCharacterStream(columnIndex);
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return getRowSet().getNCharacterStream(columnLabel);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        getRowSet().updateNCharacterStream(columnIndex, x, length);
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length)
            throws SQLException {
        getRowSet().updateNCharacterStream(columnLabel, reader, length);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        getRowSet().updateAsciiStream(columnIndex, x, length);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length)
            throws SQLException {
        getRowSet().updateBinaryStream(columnIndex, x, length);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        getRowSet().updateCharacterStream(columnIndex, x, length);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length)
            throws SQLException {
        getRowSet().updateAsciiStream(columnLabel, x, length);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length)
            throws SQLException {
        getRowSet().updateBinaryStream(columnLabel, x, length);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length)
            throws SQLException {
        getRowSet().updateCharacterStream(columnLabel, reader, length);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length)
            throws SQLException {
        getRowSet().updateBlob(columnIndex, inputStream, length);
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length)
            throws SQLException {
        getRowSet().updateBlob(columnLabel, inputStream, length);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        getRowSet().updateClob(columnIndex, reader, length);
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        getRowSet().updateClob(columnLabel, reader, length);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        getRowSet().updateNClob(columnIndex, reader, length);
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        getRowSet().updateNClob(columnLabel, reader, length);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        getRowSet().updateNCharacterStream(columnIndex, x);
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        getRowSet().updateNCharacterStream(columnLabel, reader);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        getRowSet().updateAsciiStream(columnIndex, x);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        getRowSet().updateBinaryStream(columnIndex, x);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        getRowSet().updateCharacterStream(columnIndex, x);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        getRowSet().updateBinaryStream(columnLabel, x);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        getRowSet().updateBinaryStream(columnLabel, x);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        getRowSet().updateCharacterStream(columnLabel, reader);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        getRowSet().updateBlob(columnIndex, inputStream);
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        getRowSet().updateBlob(columnLabel, inputStream);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        getRowSet().updateClob(columnIndex, reader);
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        getRowSet().updateClob(columnLabel, reader);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        getRowSet().updateNClob(columnIndex, reader);
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        getRowSet().updateNClob(columnLabel, reader);
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return getRowSet().getObject(columnIndex, type);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getRowSet().getObject(columnLabel, type);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return getRowSet().unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return getRowSet().isWrapperFor(iface);
    }
}
