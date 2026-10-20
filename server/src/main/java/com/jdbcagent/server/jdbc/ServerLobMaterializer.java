package com.jdbcagent.server.jdbc;

import com.jdbcagent.core.protocol.LobMaterializer;
import com.jdbcagent.core.protocol.LobRef;
import com.jdbcagent.core.protocol.LobTypes;

import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;

/**
 * 长度已知且不超过 {@link LobTypes#INLINE_LIMIT} 的大字段返回驱动对象，由原有逻辑嵌进 RowSet。
 * 长度未知或超出上限时，把流写入临时文件并返回 {@link LobRef}。
 */
final class ServerLobMaterializer implements LobMaterializer {
    private final long resultSetId;

    ServerLobMaterializer(long resultSetId) {
        this.resultSetId = resultSetId;
    }

    @Override
    public Object materialize(ResultSet resultSet, int columnIndex, int sqlType) throws SQLException {
        if (!LobTypes.isLob(sqlType)) {
            return PASSTHROUGH;
        }
        switch (sqlType) {
            case Types.BLOB:
                return materializeBlob(resultSet, columnIndex, sqlType);
            case Types.CLOB:
            case Types.NCLOB:
                return materializeClob(resultSet, columnIndex, sqlType);
            case Types.LONGVARBINARY:
                return materializeLongBinary(resultSet, columnIndex, sqlType);
            case Types.LONGVARCHAR:
                return materializeLongChar(resultSet, columnIndex, sqlType);
            default:
                return PASSTHROUGH;
        }
    }

    private Object materializeBlob(ResultSet resultSet, int columnIndex, int sqlType) throws SQLException {
        Blob blob = resultSet.getBlob(columnIndex);
        if (blob == null) {
            return null;
        }
        long length = lengthOf(blob);
        if (LobTypes.inline(length)) {
            return blob;
        }
        InputStream input = blob.getBinaryStream();
        return spoolBinary(input, sqlType);
    }

    private Object materializeClob(ResultSet resultSet, int columnIndex, int sqlType) throws SQLException {
        Clob clob = readClob(resultSet, columnIndex, sqlType);
        if (clob == null) {
            return null;
        }
        long length = lengthOf(clob);
        if (LobTypes.inline(length)) {
            return clob;
        }
        return spoolChars(clob.getCharacterStream(), sqlType);
    }

    private Object materializeLongBinary(ResultSet resultSet, int columnIndex, int sqlType) throws SQLException {
        InputStream input;
        try {
            input = resultSet.getBinaryStream(columnIndex);
        } catch (SQLFeatureNotSupportedException e) {
            return PASSTHROUGH;
        }
        if (input == null) {
            return null;
        }
        return spoolBinary(input, sqlType);
    }

    private Object materializeLongChar(ResultSet resultSet, int columnIndex, int sqlType) throws SQLException {
        Reader reader;
        try {
            reader = resultSet.getCharacterStream(columnIndex);
        } catch (SQLFeatureNotSupportedException e) {
            return PASSTHROUGH;
        }
        if (reader == null) {
            return null;
        }
        return spoolChars(reader, sqlType);
    }

    private static Clob readClob(ResultSet resultSet, int columnIndex, int sqlType) throws SQLException {
        if (sqlType == Types.NCLOB) {
            try {
                NClob nClob = resultSet.getNClob(columnIndex);
                return nClob;
            } catch (SQLFeatureNotSupportedException e) {
                return resultSet.getClob(columnIndex);
            }
        }
        return resultSet.getClob(columnIndex);
    }

    private LobRef spoolBinary(InputStream input, int sqlType) throws SQLException {
        LobStore.Spool spool = LobStore.spool(resultSetId, input);
        return new LobRef(spool.lobId, sqlType, spool.length, LobTypes.CHUNK_SIZE);
    }

    private LobRef spoolChars(Reader reader, int sqlType) throws SQLException {
        LobStore.Spool spool = LobStore.spool(resultSetId, reader);
        return new LobRef(spool.lobId, sqlType, spool.length, LobTypes.CHUNK_SIZE);
    }

    private static long lengthOf(Blob blob) {
        try {
            return blob.length();
        } catch (SQLException e) {
            return -1L;
        }
    }

    private static long lengthOf(Clob clob) {
        try {
            return clob.length();
        } catch (SQLException e) {
            return -1L;
        }
    }
}
