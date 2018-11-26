package com.jdbcagent.server.jdbc;

import com.jdbcagent.core.protocol.PreparedStatementMsg;
import com.jdbcagent.core.protocol.PreparedStatementMsg.ParamType;
import com.jdbcagent.core.support.serial.SerialVoid;
import com.jdbcagent.core.util.Util;

import java.io.Serializable;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Queue;

/**
 * JDBC-Agent server 端 preparedStatement 操作类
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class PreparedStatementServer extends StatementServer {

    private PreparedStatement preparedStatement;    // 实际调用的preparedStatement

    private PreparedStatement writerPStmt;

    private PreparedStatement readerPStmt;

    /**
     * 构造方法
     *
     * @param preparedStatement
     */
    public PreparedStatementServer(PreparedStatement preparedStatement,
                                   PreparedStatement writerPStmt, PreparedStatement readerPStmt) {
        super(preparedStatement, writerPStmt, readerPStmt);
        this.preparedStatement = preparedStatement;
        this.writerPStmt = writerPStmt;
        this.readerPStmt = readerPStmt;
    }

    /**
     * 关闭方法
     *
     * @throws SQLException
     */
    public void close() throws SQLException {
        super.close();
        close(preparedStatement);
        close(writerPStmt);
        close(readerPStmt);
    }

    /**
     * 公共方法调用
     *
     * @param preparedStatementMsg
     * @return
     * @throws SQLException
     */
    public Serializable preparedStatementMethod(PreparedStatementMsg preparedStatementMsg)
            throws SQLException {
        try {
            Serializable response = new SerialVoid();
            PreparedStatementMsg.Method method = preparedStatementMsg.getMethod();
            switch (method) {
                case clearParameters: {
                    preparedStatement.clearParameters();

                    if (writerPStmt != null) {
                        writerPStmt.clearParameters();
                    }
                    if (readerPStmt != null) {
                        readerPStmt.clearParameters();
                    }
                    break;
                }
                case addBatch: {
                    PreparedStatement pstmt = preparedStatement;
                    if (writerPStmt != null) {
                        pstmt = writerPStmt;
                    }
                    pstmt.addBatch();
                    break;
                }
                case execute: {
                    PreparedStatement pstmt = preparedStatement;
                    if (writerPStmt != null) {
                        pstmt = writerPStmt;
                    }

                    // noinspection unchecked
                    Queue<PreparedStatementMsg> paramsQueue =
                            (Queue<PreparedStatementMsg>) preparedStatementMsg.getParams()[0];
                    setParams(pstmt, paramsQueue);
                    response = pstmt.execute();
                    break;
                }
                case executeUpdate: {
                    PreparedStatement pstmt = preparedStatement;
                    if (writerPStmt != null) {
                        pstmt = writerPStmt;
                    }

                    // noinspection unchecked
                    Queue<PreparedStatementMsg> paramsQueue =
                            (Queue<PreparedStatementMsg>) preparedStatementMsg.getParams()[0];
                    setParams(pstmt, paramsQueue);
                    response = pstmt.executeUpdate();
                    break;
                }
                case executeQuery: {
                    PreparedStatement pstmt = preparedStatement;
                    if (readerPStmt != null) {
                        pstmt = readerPStmt;
                    }

                    // noinspection unchecked
                    Queue<PreparedStatementMsg> paramsQueue =
                            (Queue<PreparedStatementMsg>) preparedStatementMsg.getParams()[0];
                    setParams(pstmt, paramsQueue);
                    ResultSetServer resultSetServer =
                            new ResultSetServer(pstmt.executeQuery());
                    response = resultSetServer.currentId;
                    break;
                }


                case getMaxFieldSize: {
                    response = preparedStatement.getMaxFieldSize();
                    break;
                }
                case setMaxFieldSize: {
                    int max = (Integer) preparedStatementMsg.getParams()[0];
                    preparedStatement.setMaxFieldSize(max);

                    if (writerPStmt != null) {
                        writerPStmt.setMaxFieldSize(max);
                    }
                    if (readerPStmt != null) {
                        readerPStmt.setMaxFieldSize(max);
                    }
                    break;
                }
                case getMaxRows: {
                    response = preparedStatement.getMaxRows();
                    break;
                }
                case setMaxRows: {
                    int max = (Integer) preparedStatementMsg.getParams()[0];
                    preparedStatement.setMaxRows(max);

                    if (writerPStmt != null) {
                        writerPStmt.setMaxRows(max);
                    }
                    if (readerPStmt != null) {
                        readerPStmt.setMaxRows(max);
                    }
                    break;
                }
                case setEscapeProcessing: {
                    boolean enable = (Boolean) preparedStatementMsg.getParams()[0];
                    preparedStatement.setEscapeProcessing(enable);

                    if (writerPStmt != null) {
                        writerPStmt.setEscapeProcessing(enable);
                    }
                    if (readerPStmt != null) {
                        readerPStmt.setEscapeProcessing(enable);
                    }
                    break;
                }
                case getQueryTimeout: {
                    response = preparedStatement.getQueryTimeout();
                    break;
                }
                case setQueryTimeout: {
                    int seconds = (Integer) preparedStatementMsg.getParams()[0];
                    preparedStatement.setQueryTimeout(seconds);

                    if (writerPStmt != null) {
                        writerPStmt.setQueryTimeout(seconds);
                    }
                    if (readerPStmt != null) {
                        readerPStmt.setQueryTimeout(seconds);
                    }
                    break;
                }
                case cancel: {
                    preparedStatement.cancel();

                    if (writerPStmt != null) {
                        writerPStmt.cancel();
                    }
                    if (readerPStmt != null) {
                        readerPStmt.cancel();
                    }
                    break;
                }
                case getWarnings: {
                    SQLWarning sqlWarning = preparedStatement.getWarnings();
                    if (sqlWarning != null) {
                        response = sqlWarning.getMessage();
                    } else {
                        response = null;
                    }
                    break;
                }
                case clearWarnings: {
                    preparedStatement.clearWarnings();

                    if (writerPStmt != null) {
                        writerPStmt.clearWarnings();
                    }
                    if (readerPStmt != null) {
                        readerPStmt.clearWarnings();
                    }
                    break;
                }
                case setCursorName: {
                    String name = (String) preparedStatementMsg.getParams()[0];
                    preparedStatement.setCursorName(name);

                    if (writerPStmt != null) {
                        writerPStmt.setCursorName(name);
                    }
                    if (readerPStmt != null) {
                        readerPStmt.setCursorName(name);
                    }
                    break;
                }

                case getUpdateCount: {
                    response = preparedStatement.getUpdateCount();
                    break;
                }
                case getMoreResults: {
                    int len = preparedStatementMsg.getParams().length;
                    if (len == 0) {
                        response = preparedStatement.getMoreResults();
                    } else if (len == 1) {
                        int current = (Integer) preparedStatementMsg.getParams()[0];
                        response = preparedStatement.getMoreResults(current);
                    }
                    break;
                }
                case setFetchDirection: {
                    int direction = (Integer) preparedStatementMsg.getParams()[0];
                    preparedStatement.setFetchDirection(direction);

                    if (writerPStmt != null) {
                        writerPStmt.setFetchDirection(direction);
                    }
                    if (readerPStmt != null) {
                        readerPStmt.setFetchDirection(direction);
                    }
                    break;
                }
                case getFetchDirection: {
                    response = preparedStatement.getFetchDirection();
                    break;
                }
                case setFetchSize: {
                    int rows = (Integer) preparedStatementMsg.getParams()[0];
                    preparedStatement.setFetchSize(rows);

                    if (writerPStmt != null) {
                        writerPStmt.setFetchSize(rows);
                    }
                    if (readerPStmt != null) {
                        readerPStmt.setFetchSize(rows);
                    }
                    break;
                }
                case getFetchSize: {
                    response = preparedStatement.getFetchSize();
                    break;
                }
                case getResultSetConcurrency: {
                    response = preparedStatement.getResultSetConcurrency();
                    break;
                }
                case getResultSetType: {
                    response = preparedStatement.getResultSetType();
                    break;
                }

                case clearBatch: {
                    Statement stmt = preparedStatement;
                    if (writerPStmt != null) {
                        stmt = writerPStmt;
                    }
                    stmt.clearBatch();
                    break;
                }
                case executeBatch: {
                    Statement stmt = preparedStatement;
                    if (writerPStmt != null) {
                        stmt = writerPStmt;
                    }
                    response = stmt.executeBatch();
                    break;
                }
                case getResultSetHoldability: {
                    response = preparedStatement.getResultSetHoldability();
                    break;
                }
                case isClosed: {
                    response = preparedStatement.isClosed();
                    break;
                }
                case setPoolable: {
                    boolean poolable = (Boolean) preparedStatementMsg.getParams()[0];
                    preparedStatement.setPoolable(poolable);
                    if (writerPStmt != null) {
                        writerPStmt.setPoolable(poolable);
                    }
                    if (readerPStmt != null) {
                        readerPStmt.setPoolable(poolable);
                    }
                    break;
                }
                case isPoolable: {
                    response = preparedStatement.isPoolable();
                    break;
                }
                case closeOnCompletion: {
                    preparedStatement.closeOnCompletion();
                    if (writerPStmt != null) {
                        writerPStmt.closeOnCompletion();
                    }
                    if (readerPStmt != null) {
                        readerPStmt.closeOnCompletion();
                    }
                    break;
                }
                case isCloseOnCompletion: {
                    response = preparedStatement.isCloseOnCompletion();
                    break;
                }
            }
            return response;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    protected void setParams(PreparedStatement pstmt, Queue<PreparedStatementMsg> paramsQueue) throws SQLException {
        if (paramsQueue == null) {
            return;
        }
        for (; ; ) {
            PreparedStatementMsg preparedStatementMsg = paramsQueue.poll();
            if (preparedStatementMsg == null) {
                break;
            }
            ParamType paramType = preparedStatementMsg.getParamType();
            int parameterIndex = preparedStatementMsg.getParameterIndex();
            Serializable[] params = preparedStatementMsg.getParams();
            int len = 0;
            if (params != null) {
                len = params.length;
            }
            switch (paramType) {
                case NULL:
                    if (len == 2) {
                        int sqlType = (Integer) params[0];
                        String typeName = (String) params[1];
                        pstmt.setNull(parameterIndex, sqlType, typeName);
                    } else if (len == 1) {
                        pstmt.setNull(parameterIndex, (Integer) params[0]);
                    }
                    break;
                case BOOLEAN:
                    if (len == 1) {
                        pstmt.setBoolean(parameterIndex, (Boolean) params[0]);
                    }
                    break;
                case BYTE:
                    if (len == 1) {
                        pstmt.setByte(parameterIndex, (Byte) params[0]);
                    }
                    break;
                case SHORT:
                    if (len == 1) {
                        pstmt.setShort(parameterIndex, (Short) params[0]);
                    }
                    break;
                case INT:
                    if (len == 1) {
                        pstmt.setInt(parameterIndex, (Integer) params[0]);
                    }
                    break;
                case LONG:
                    if (len == 1) {
                        pstmt.setLong(parameterIndex, (Long) params[0]);
                    }
                    break;
                case FLOAT:
                    if (len == 1) {
                        pstmt.setFloat(parameterIndex, (Float) params[0]);
                    }
                    break;
                case DOUBLE:
                    if (len == 1) {
                        pstmt.setDouble(parameterIndex, (Double) params[0]);
                    }
                    break;
                case BIG_DECIMAL:
                    if (len == 1) {
                        pstmt.setBigDecimal(parameterIndex, (BigDecimal) params[0]);
                    }
                    break;
                case STRING:
                    if (len == 1) {
                        pstmt.setString(parameterIndex, (String) params[0]);
                    }
                    break;
                case BYTES:
                    if (len == 1) {
                        pstmt.setBytes(parameterIndex, (byte[]) params[0]);
                    }
                    break;
                case DATE:
                    if (len == 2) {
                        Date value = (Date) params[0];
                        Calendar calendar = (Calendar) params[1];
                        pstmt.setDate(parameterIndex, value, calendar);
                    } else if (len == 1) {
                        pstmt.setDate(parameterIndex, (Date) params[0]);
                    }
                    break;
                case TIME:
                    if (len == 2) {
                        Time value = (Time) params[0];
                        Calendar calendar = (Calendar) params[1];
                        pstmt.setTime(parameterIndex, value, calendar);
                    } else if (len == 1) {
                        pstmt.setTime(parameterIndex, (Time) params[0]);
                    }
                    break;
                case TIMESTAMP:
                    if (len == 2) {
                        Timestamp value = (Timestamp) params[0];
                        Calendar calendar = (Calendar) params[1];
                        pstmt.setTimestamp(parameterIndex, value, calendar);
                    } else if (len == 1) {
                        pstmt.setTimestamp(parameterIndex, (Timestamp) params[0]);
                    }
                    break;
                case ASCII_STREAM:
                    if (len == 2) {
                        byte[] buf = (byte[]) params[0];
                        if (params[1] instanceof Integer) {
                            int length = (Integer) params[1];
                            pstmt.setAsciiStream(parameterIndex, Util.byte2Input(buf),
                                    length);
                        } else if (params[1] instanceof Long) {
                            long length = (Long) params[1];
                            pstmt.setAsciiStream(parameterIndex, Util.byte2Input(buf),
                                    length);
                        }
                    } else if (len == 1) {
                        pstmt.setAsciiStream(parameterIndex,
                                Util.byte2Input((byte[]) params[0]));
                    }
                    break;
                case UNICODE_STREAM:
                    if (len == 1) {
                        byte[] buf = (byte[]) params[0];
                        int length = (Integer) params[1];
                        pstmt.setUnicodeStream(parameterIndex, Util.byte2Input(buf),
                                length);
                    }
                    break;
                case BINARY_STREAM:
                    if (len == 2) {
                        byte[] buf = (byte[]) params[0];
                        if (params[1] instanceof Integer) {
                            int length = (Integer) params[1];
                            pstmt.setBinaryStream(parameterIndex, Util.byte2Input(buf),
                                    length);
                        } else if (params[1] instanceof Long) {
                            long length = (Long) params[1];
                            pstmt.setBinaryStream(parameterIndex, Util.byte2Input(buf),
                                    length);
                        }
                    } else if (len == 1) {
                        pstmt.setBinaryStream(parameterIndex,
                                Util.byte2Input((byte[]) params[0]));
                    }
                    break;
                case OBJECT:
                    if (len == 2) {
                        Object value = params[0];
                        int targetSqlType = (Integer) params[1];
                        pstmt.setObject(parameterIndex, value, targetSqlType);
                    } else if (params != null && params.length == 3) {
                        Object value = params[0];
                        int targetSqlType = (Integer) params[1];
                        int scaleOrLength = (Integer) params[2];
                        pstmt.setObject(parameterIndex, value, targetSqlType,
                                scaleOrLength);
                    } else if (len == 1) {
                        pstmt.setObject(parameterIndex, params[0]);
                    }
                    break;
                case CHARACTER_STREAM:
                    if (params != null && params.length == 2) {
                        String str = (String) params[0];
                        if (params[1] instanceof Integer) {
                            int length = (Integer) params[1];
                            pstmt.setCharacterStream(parameterIndex,
                                    Util.string2Reader(str), length);
                        } else if (params[1] instanceof Long) {
                            long length = (Long) params[1];
                            pstmt.setCharacterStream(parameterIndex,
                                    Util.string2Reader(str), length);
                        }
                    } else if (len == 1) {
                        pstmt.setCharacterStream(parameterIndex,
                                Util.string2Reader((String) params[0]));
                    }
                    break;
                case REF:
                    if (len == 1) {
                        pstmt.setRef(parameterIndex, (Ref) params[0]);
                    }
                    break;
                case BLOB:
                    if (len == 1) {
                        pstmt.setBlob(parameterIndex, (Blob) params[0]);
                    }
                    break;
                case CLOB:
                    if (len == 1) {
                        pstmt.setClob(parameterIndex, (Clob) params[0]);
                    }
                    break;
                case ARRAY:
                    if (len == 1) {
                        pstmt.setArray(parameterIndex, (Array) params[0]);
                    }
                    break;
                case URL:
                    if (len == 1) {
                        pstmt.setURL(parameterIndex, (URL) params[0]);
                    }
                    break;
                case ROW_ID:
                    if (len == 1) {
                        pstmt.setRowId(parameterIndex, (RowId) params[0]);
                    }
                    break;
                case NSTRING:
                    if (len == 1) {
                        pstmt.setNString(parameterIndex, (String) params[0]);
                    }
                    break;
                case NCHARACTER_STREAM:
                    if (len == 2) {
                        String str = (String) params[0];
                        int length = (Integer) params[1];
                        pstmt.setNCharacterStream(parameterIndex,
                                Util.string2Reader(str), length);
                    } else if (len == 1) {
                        pstmt.setNCharacterStream(parameterIndex,
                                Util.string2Reader((String) params[0]));
                    }
                    break;
                case NCLOB:
                    if (len == 1) {
                        pstmt.setNClob(parameterIndex, (NClob) params[0]);
                    }
                    break;
            }
        }

    }
}
