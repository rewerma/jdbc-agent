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

    /**
     * 构造方法
     *
     * @param preparedStatement
     */
    public PreparedStatementServer(PreparedStatement preparedStatement) {
        super(preparedStatement);
        this.preparedStatement = preparedStatement;
    }

    /**
     * 关闭方法
     *
     * @throws SQLException
     */
    public void close() throws SQLException {
        super.close();
        if (preparedStatement != null && !preparedStatement.isClosed()) {
            preparedStatement.close();
            preparedStatement = null;
        }
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
                    break;
                }
                case addBatch: {
                    preparedStatement.addBatch();
                    break;
                }
                case execute: {
                    // noinspection unchecked
                    Queue<PreparedStatementMsg> paramsQueue =
                            (Queue<PreparedStatementMsg>) preparedStatementMsg.getParams()[0];
                    setParams(paramsQueue);
                    response = preparedStatement.execute();
                    break;
                }
                case executeUpdate: {
                    // noinspection unchecked
                    Queue<PreparedStatementMsg> paramsQueue =
                            (Queue<PreparedStatementMsg>) preparedStatementMsg.getParams()[0];
                    setParams(paramsQueue);
                    response = preparedStatement.executeUpdate();
                    break;
                }
                case executeQuery: {
                    // noinspection unchecked
                    Queue<PreparedStatementMsg> paramsQueue =
                            (Queue<PreparedStatementMsg>) preparedStatementMsg.getParams()[0];
                    setParams(paramsQueue);
                    ResultSetServer resultSetServer =
                            new ResultSetServer(preparedStatement.executeQuery());
                    response = resultSetServer.currentId;
                    break;
                }
            }
            return response;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    protected void setParams(Queue<PreparedStatementMsg> paramsQueue) throws SQLException {
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
                        preparedStatement.setNull(parameterIndex, sqlType, typeName);
                    } else if (len == 1) {
                        preparedStatement.setNull(parameterIndex, (Integer) params[0]);
                    }
                    break;
                case BOOLEAN:
                    if (len == 1) {
                        preparedStatement.setBoolean(parameterIndex, (Boolean) params[0]);
                    }
                    break;
                case BYTE:
                    if (len == 1) {
                        preparedStatement.setByte(parameterIndex, (Byte) params[0]);
                    }
                    break;
                case SHORT:
                    if (len == 1) {
                        preparedStatement.setShort(parameterIndex, (Short) params[0]);
                    }
                    break;
                case INT:
                    if (len == 1) {
                        preparedStatement.setInt(parameterIndex, (Integer) params[0]);
                    }
                    break;
                case LONG:
                    if (len == 1) {
                        preparedStatement.setLong(parameterIndex, (Long) params[0]);
                    }
                    break;
                case FLOAT:
                    if (len == 1) {
                        preparedStatement.setFloat(parameterIndex, (Float) params[0]);
                    }
                    break;
                case DOUBLE:
                    if (len == 1) {
                        preparedStatement.setDouble(parameterIndex, (Double) params[0]);
                    }
                    break;
                case BIG_DECIMAL:
                    if (len == 1) {
                        preparedStatement.setBigDecimal(parameterIndex, (BigDecimal) params[0]);
                    }
                    break;
                case STRING:
                    if (len == 1) {
                        preparedStatement.setString(parameterIndex, (String) params[0]);
                    }
                    break;
                case BYTES:
                    if (len == 1) {
                        preparedStatement.setBytes(parameterIndex, (byte[]) params[0]);
                    }
                    break;
                case DATE:
                    if (len == 2) {
                        Date value = (Date) params[0];
                        Calendar calendar = (Calendar) params[1];
                        preparedStatement.setDate(parameterIndex, value, calendar);
                    } else if (len == 1) {
                        preparedStatement.setDate(parameterIndex, (Date) params[0]);
                    }
                    break;
                case TIME:
                    if (len == 2) {
                        Time value = (Time) params[0];
                        Calendar calendar = (Calendar) params[1];
                        preparedStatement.setTime(parameterIndex, value, calendar);
                    } else if (len == 1) {
                        preparedStatement.setTime(parameterIndex, (Time) params[0]);
                    }
                    break;
                case TIMESTAMP:
                    if (len == 2) {
                        Timestamp value = (Timestamp) params[0];
                        Calendar calendar = (Calendar) params[1];
                        preparedStatement.setTimestamp(parameterIndex, value, calendar);
                    } else if (len == 1) {
                        preparedStatement.setTimestamp(parameterIndex, (Timestamp) params[0]);
                    }
                    break;
                case ASCII_STREAM:
                    if (len == 2) {
                        byte[] buf = (byte[]) params[0];
                        if (params[1] instanceof Integer) {
                            int length = (Integer) params[1];
                            preparedStatement.setAsciiStream(parameterIndex, Util.byte2Input(buf),
                                    length);
                        } else if (params[1] instanceof Long) {
                            long length = (Long) params[1];
                            preparedStatement.setAsciiStream(parameterIndex, Util.byte2Input(buf),
                                    length);
                        }
                    } else if (len == 1) {
                        preparedStatement.setAsciiStream(parameterIndex,
                                Util.byte2Input((byte[]) params[0]));
                    }
                    break;
                case UNICODE_STREAM:
                    if (len == 1) {
                        byte[] buf = (byte[]) params[0];
                        int length = (Integer) params[1];
                        preparedStatement.setUnicodeStream(parameterIndex, Util.byte2Input(buf),
                                length);
                    }
                    break;
                case BINARY_STREAM:
                    if (len == 2) {
                        byte[] buf = (byte[]) params[0];
                        if (params[1] instanceof Integer) {
                            int length = (Integer) params[1];
                            preparedStatement.setBinaryStream(parameterIndex, Util.byte2Input(buf),
                                    length);
                        } else if (params[1] instanceof Long) {
                            long length = (Long) params[1];
                            preparedStatement.setBinaryStream(parameterIndex, Util.byte2Input(buf),
                                    length);
                        }
                    } else if (len == 1) {
                        preparedStatement.setBinaryStream(parameterIndex,
                                Util.byte2Input((byte[]) params[0]));
                    }
                    break;
                case OBJECT:
                    if (len == 2) {
                        Object value = params[0];
                        int targetSqlType = (Integer) params[1];
                        preparedStatement.setObject(parameterIndex, value, targetSqlType);
                    } else if (params != null && params.length == 3) {
                        Object value = params[0];
                        int targetSqlType = (Integer) params[1];
                        int scaleOrLength = (Integer) params[2];
                        preparedStatement.setObject(parameterIndex, value, targetSqlType,
                                scaleOrLength);
                    } else if (len == 1) {
                        preparedStatement.setObject(parameterIndex, params[0]);
                    }
                    break;
                case CHARACTER_STREAM:
                    if (params != null && params.length == 2) {
                        String str = (String) params[0];
                        if (params[1] instanceof Integer) {
                            int length = (Integer) params[1];
                            preparedStatement.setCharacterStream(parameterIndex,
                                    Util.string2Reader(str), length);
                        } else if (params[1] instanceof Long) {
                            long length = (Long) params[1];
                            preparedStatement.setCharacterStream(parameterIndex,
                                    Util.string2Reader(str), length);
                        }
                    } else if (len == 1) {
                        preparedStatement.setCharacterStream(parameterIndex,
                                Util.string2Reader((String) params[0]));
                    }
                    break;
                case REF:
                    if (len == 1) {
                        preparedStatement.setRef(parameterIndex, (Ref) params[0]);
                    }
                    break;
                case BLOB:
                    if (len == 1) {
                        preparedStatement.setBlob(parameterIndex, (Blob) params[0]);
                    }
                    break;
                case CLOB:
                    if (len == 1) {
                        preparedStatement.setClob(parameterIndex, (Clob) params[0]);
                    }
                    break;
                case ARRAY:
                    if (len == 1) {
                        preparedStatement.setArray(parameterIndex, (Array) params[0]);
                    }
                    break;
                case URL:
                    if (len == 1) {
                        preparedStatement.setURL(parameterIndex, (URL) params[0]);
                    }
                    break;
                case ROW_ID:
                    if (len == 1) {
                        preparedStatement.setRowId(parameterIndex, (RowId) params[0]);
                    }
                    break;
                case NSTRING:
                    if (len == 1) {
                        preparedStatement.setNString(parameterIndex, (String) params[0]);
                    }
                    break;
                case NCHARACTER_STREAM:
                    if (len == 2) {
                        String str = (String) params[0];
                        int length = (Integer) params[1];
                        preparedStatement.setNCharacterStream(parameterIndex,
                                Util.string2Reader(str), length);
                    } else if (len == 1) {
                        preparedStatement.setNCharacterStream(parameterIndex,
                                Util.string2Reader((String) params[0]));
                    }
                    break;
                case NCLOB:
                    if (len == 1) {
                        preparedStatement.setNClob(parameterIndex, (NClob) params[0]);
                    }
                    break;
            }
        }

    }
}
