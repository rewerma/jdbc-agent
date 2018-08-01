package com.jdbcagent.server.jdbc;

import com.jdbcagent.core.protocol.CallableStatementMsg;
import com.jdbcagent.core.protocol.CallableStatementMsg.Method;
import com.jdbcagent.core.protocol.PreparedStatementMsg;
import com.jdbcagent.core.protocol.PreparedStatementMsg.ParamType;
import com.jdbcagent.core.support.serial.SerialNClob;
import com.jdbcagent.core.support.serial.SerialRowId;
import com.jdbcagent.core.support.serial.SerialVoid;
import com.jdbcagent.core.util.Util;

import javax.sql.rowset.serial.SerialArray;
import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import javax.sql.rowset.serial.SerialRef;
import java.io.Reader;
import java.io.Serializable;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Queue;

/**
 * JDBC-Agent server 端 callableStatement 操作类
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class CallableStatementServer extends PreparedStatementServer {

    private CallableStatement callableStatement;    // 实际调用的callableStatement

    /**
     * 构造方法
     *
     * @param callableStatement
     */
    public CallableStatementServer(CallableStatement callableStatement) {
        super(callableStatement);
        this.callableStatement = callableStatement;
    }

    /**
     * 关闭方法
     *
     * @throws SQLException
     */
    public void close() throws SQLException {
        super.close();
        if (callableStatement != null && !callableStatement.isClosed()) {
            callableStatement.close();
            callableStatement = null;
        }
    }

    /**
     * 公共方法调用
     *
     * @param callableStatementMsg 调用信息
     * @return 返回结果
     * @throws SQLException
     */
    @SuppressWarnings("unchecked")
    public Serializable preparedStatementMethod(CallableStatementMsg callableStatementMsg) throws SQLException {
        try {
            Serializable response = new SerialVoid();
            Method method = callableStatementMsg.getMethod();

            Queue<PreparedStatementMsg> paramsQueue = null;
            Queue<CallableStatementMsg> csParamsQueue = null;
            if (callableStatementMsg.getParams() != null) {
                paramsQueue = (Queue<PreparedStatementMsg>) callableStatementMsg.getParams()[0];
                csParamsQueue = (Queue<CallableStatementMsg>) callableStatementMsg.getParams()[1];
                setParams(paramsQueue);
                setCSParams(csParamsQueue);
            }

            switch (method) {
                case clearParameters: {
                    callableStatement.clearParameters();
                    break;
                }
                case execute: {
                    if (csParamsQueue == null || paramsQueue == null) {
                        throw new IllegalArgumentException();
                    }
                    response = callableStatement.execute();
                    break;
                }
                case executeUpdate: {
                    if (csParamsQueue == null || paramsQueue == null) {
                        throw new IllegalArgumentException();
                    }
                    response = callableStatement.executeUpdate();
                    break;
                }
                case executeQuery: {
                    if (csParamsQueue == null || paramsQueue == null) {
                        throw new IllegalArgumentException();
                    }
                    ResultSetServer resultSetServer = new ResultSetServer(callableStatement.executeQuery());
                    response = resultSetServer.currentId;
                    break;
                }
                default: {
                    response = getResult(method, callableStatementMsg.getParams());
                    break;
                }
            }
            return response;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    private void setCSParams(Queue<CallableStatementMsg> paramsQueue) throws SQLException {
        if (paramsQueue == null) {
            return;
        }
        for (; ; ) {
            CallableStatementMsg callableStatementMsg = paramsQueue.poll();
            if (callableStatementMsg == null) {
                break;
            }
            ParamType paramType = callableStatementMsg.getParamType();
            String parameterName = callableStatementMsg.getParameterName();
            Serializable[] params = callableStatementMsg.getParams();
            int len = 0;
            if (params != null) {
                len = params.length;
            }
            switch (paramType) {
                case NULL:
                    if (len == 2) {
                        int sqlType = (Integer) params[0];
                        String typeName = (String) params[1];
                        callableStatement.setNull(parameterName, sqlType, typeName);
                    } else if (len == 1) {
                        callableStatement.setNull(parameterName, (Integer) params[0]);
                    }
                    break;
                case BOOLEAN:
                    if (len == 1) {
                        callableStatement.setBoolean(parameterName, (Boolean) params[0]);
                    }
                    break;
                case BYTE:
                    if (len == 1) {
                        callableStatement.setByte(parameterName, (Byte) params[0]);
                    }
                    break;
                case SHORT:
                    if (len == 1) {
                        callableStatement.setShort(parameterName, (Short) params[0]);
                    }
                    break;
                case INT:
                    if (len == 1) {
                        callableStatement.setInt(parameterName, (Integer) params[0]);
                    }
                    break;
                case LONG:
                    if (len == 1) {
                        callableStatement.setLong(parameterName, (Long) params[0]);
                    }
                    break;
                case FLOAT:
                    if (len == 1) {
                        callableStatement.setFloat(parameterName, (Float) params[0]);
                    }
                    break;
                case DOUBLE:
                    if (len == 1) {
                        callableStatement.setDouble(parameterName, (Double) params[0]);
                    }
                    break;
                case BIG_DECIMAL:
                    if (len == 1) {
                        callableStatement.setBigDecimal(parameterName, (BigDecimal) params[0]);
                    }
                    break;
                case STRING:
                    if (len == 1) {
                        callableStatement.setString(parameterName, (String) params[0]);
                    }
                    break;
                case BYTES:
                    if (len == 1) {
                        callableStatement.setBytes(parameterName, (byte[]) params[0]);
                    }
                    break;
                case DATE:
                    if (len == 2) {
                        Date value = (Date) params[0];
                        Calendar calendar = (Calendar) params[1];
                        callableStatement.setDate(parameterName, value, calendar);
                    } else if (len == 1) {
                        callableStatement.setDate(parameterName, (Date) params[0]);
                    }
                    break;
                case TIME:
                    if (len == 2) {
                        Time value = (Time) params[0];
                        Calendar calendar = (Calendar) params[1];
                        callableStatement.setTime(parameterName, value, calendar);
                    } else if (len == 1) {
                        callableStatement.setTime(parameterName, (Time) params[0]);
                    }
                    break;
                case TIMESTAMP:
                    if (len == 2) {
                        Timestamp value = (Timestamp) params[0];
                        Calendar calendar = (Calendar) params[1];
                        callableStatement.setTimestamp(parameterName, value, calendar);
                    } else if (len == 1) {
                        callableStatement.setTimestamp(parameterName, (Timestamp) params[0]);
                    }
                    break;
                case ASCII_STREAM:
                    if (len == 2) {
                        byte[] buf = (byte[]) params[0];
                        if (params[1] instanceof Integer) {
                            int length = (Integer) params[1];
                            callableStatement.setAsciiStream(parameterName, Util.byte2Input(buf),
                                    length);
                        } else if (params[1] instanceof Long) {
                            long length = (Long) params[1];
                            callableStatement.setAsciiStream(parameterName, Util.byte2Input(buf),
                                    length);
                        }
                    } else if (len == 1) {
                        callableStatement.setAsciiStream(parameterName,
                                Util.byte2Input((byte[]) params[0]));
                    }
                    break;
                case BINARY_STREAM:
                    if (len == 2) {
                        byte[] buf = (byte[]) params[0];
                        if (params[1] instanceof Integer) {
                            int length = (Integer) params[1];
                            callableStatement.setBinaryStream(parameterName, Util.byte2Input(buf),
                                    length);
                        } else if (params[1] instanceof Long) {
                            long length = (Long) params[1];
                            callableStatement.setBinaryStream(parameterName, Util.byte2Input(buf),
                                    length);
                        }
                    } else if (len == 1) {
                        callableStatement.setBinaryStream(parameterName,
                                Util.byte2Input((byte[]) params[0]));
                    }
                    break;
                case OBJECT:
                    if (len == 2) {
                        Object value = params[0];
                        int targetSqlType = (Integer) params[1];
                        callableStatement.setObject(parameterName, value, targetSqlType);
                    } else if (params != null && params.length == 3) {
                        Object value = params[0];
                        int targetSqlType = (Integer) params[1];
                        int scaleOrLength = (Integer) params[2];
                        callableStatement.setObject(parameterName, value, targetSqlType,
                                scaleOrLength);
                    } else if (len == 1) {
                        callableStatement.setObject(parameterName, params[0]);
                    }
                    break;
                case CHARACTER_STREAM:
                    if (params != null && params.length == 2) {
                        String str = (String) params[0];
                        if (params[1] instanceof Integer) {
                            int length = (Integer) params[1];
                            callableStatement.setCharacterStream(parameterName,
                                    Util.string2Reader(str), length);
                        } else if (params[1] instanceof Long) {
                            long length = (Long) params[1];
                            callableStatement.setCharacterStream(parameterName,
                                    Util.string2Reader(str), length);
                        }
                    } else if (len == 1) {
                        callableStatement.setCharacterStream(parameterName,
                                Util.string2Reader((String) params[0]));
                    }
                    break;
                case BLOB:
                    if (len == 1) {
                        callableStatement.setBlob(parameterName, (Blob) params[0]);
                    }
                    break;
                case CLOB:
                    if (len == 1) {
                        callableStatement.setClob(parameterName, (Clob) params[0]);
                    }
                    break;
                case URL:
                    if (len == 1) {
                        callableStatement.setURL(parameterName, (URL) params[0]);
                    }
                    break;
                case ROW_ID:
                    if (len == 1) {
                        callableStatement.setRowId(parameterName, (RowId) params[0]);
                    }
                    break;
                case NSTRING:
                    if (len == 1) {
                        callableStatement.setNString(parameterName, (String) params[0]);
                    }
                    break;
                case NCHARACTER_STREAM:
                    if (len == 2) {
                        String str = (String) params[0];
                        int length = (Integer) params[1];
                        callableStatement.setNCharacterStream(parameterName,
                                Util.string2Reader(str), length);
                    } else if (len == 1) {
                        callableStatement.setNCharacterStream(parameterName,
                                Util.string2Reader((String) params[0]));
                    }
                    break;
                case NCLOB:
                    if (len == 1) {
                        callableStatement.setNClob(parameterName, (NClob) params[0]);
                    }
                    break;
            }
        }
    }

    private Serializable getResult(Method method, Serializable... params) throws SQLException {
        Serializable result = null;
        Object param1 = null;
        int len = 0;
        if (params != null) {
            len = params.length;
        }
        if (len > 0) {
            param1 = params[0];
        }
        try {
            switch (method) {
                case wasNull: {
                    result = callableStatement.wasNull();
                    break;
                }
                case getString: {
                    if (param1 instanceof String) {
                        result = callableStatement.getString((String) param1);
                    } else if (param1 instanceof Integer) {
                        result = callableStatement.getString((Integer) param1);
                    }
                    break;
                }
                case getBoolean: {
                    if (param1 instanceof String) {
                        result = callableStatement.getBoolean((String) param1);
                    } else if (param1 instanceof Integer) {
                        result = callableStatement.getBoolean((Integer) param1);
                    }
                    break;
                }
                case getByte: {
                    if (param1 instanceof String) {
                        result = callableStatement.getByte((String) param1);
                    } else if (param1 instanceof Integer) {
                        result = callableStatement.getByte((Integer) param1);
                    }
                    break;
                }
                case getShort: {
                    if (param1 instanceof String) {
                        result = callableStatement.getShort((String) param1);
                    } else if (param1 instanceof Integer) {
                        result = callableStatement.getShort((Integer) param1);
                    }
                    break;
                }
                case getInt: {
                    if (param1 instanceof String) {
                        result = callableStatement.getInt((String) param1);
                    } else if (param1 instanceof Integer) {
                        result = callableStatement.getInt((Integer) param1);
                    }
                    break;
                }
                case getLong: {
                    if (param1 instanceof String) {
                        result = callableStatement.getLong((String) param1);
                    } else if (param1 instanceof Integer) {
                        result = callableStatement.getLong((Integer) param1);
                    }
                    break;
                }
                case getFloat: {
                    if (param1 instanceof String) {
                        result = callableStatement.getFloat((String) param1);
                    } else if (param1 instanceof Integer) {
                        result = callableStatement.getFloat((Integer) param1);
                    }
                    break;
                }
                case getDouble: {
                    if (param1 instanceof String) {
                        result = callableStatement.getDouble((String) param1);
                    } else if (param1 instanceof Integer) {
                        result = callableStatement.getDouble((Integer) param1);
                    }
                    break;
                }
                case getBigDecimal: {
                    Integer param2 = null;
                    if (len == 2) {
                        param2 = (Integer) params[1];
                    }
                    if (param1 instanceof String) {
                        result = callableStatement.getBigDecimal((String) param1);
                    } else if (param1 instanceof Integer) {
                        if (param2 != null) {
                            result = callableStatement.getBigDecimal((Integer) param1, param2);
                        } else {
                            result = callableStatement.getBigDecimal((Integer) param1);
                        }
                    }
                    break;
                }
                case getBytes: {
                    if (param1 instanceof String) {
                        result = callableStatement.getBytes((String) param1);
                    } else if (param1 instanceof Integer) {
                        result = callableStatement.getBytes((Integer) param1);
                    }
                    break;
                }
                case getDate: {
                    if (param1 instanceof String) {
                        result = callableStatement.getDate((String) param1);
                    } else if (param1 instanceof Integer) {
                        result = callableStatement.getDate((Integer) param1);
                    }
                    break;
                }
                case getTime: {
                    if (param1 instanceof String) {
                        result = callableStatement.getTime((String) param1);
                    } else if (param1 instanceof Integer) {
                        result = callableStatement.getTime((Integer) param1);
                    }
                    break;
                }
                case getTimestamp: {
                    if (param1 instanceof String) {
                        result = callableStatement.getTimestamp((String) param1);
                    } else if (param1 instanceof Integer) {
                        result = callableStatement.getTimestamp((Integer) param1);
                    }
                    break;
                }
                case getObject: {
                    if (param1 instanceof String) {
                        result = (Serializable) callableStatement.getObject((String) param1);
                    } else if (param1 instanceof Integer) {
                        result = (Serializable) callableStatement.getObject((Integer) param1);
                    }
                    break;
                }
                case getRef: {
                    Ref ref = null;
                    if (param1 instanceof String) {
                        ref = callableStatement.getRef((String) param1);
                    } else if (param1 instanceof Integer) {
                        ref = callableStatement.getRef((Integer) param1);
                    }
                    if (ref != null) {
                        result = new SerialRef(ref);
                    }
                    break;
                }
                case getBlob: {
                    Blob blob = null;
                    if (param1 instanceof String) {
                        blob = callableStatement.getBlob((String) param1);
                    } else if (param1 instanceof Integer) {
                        blob = callableStatement.getBlob((Integer) param1);
                    }
                    if (blob != null) {
                        result = new SerialBlob(blob);
                    }
                    break;
                }
                case getClob: {
                    Clob clob = null;
                    if (param1 instanceof String) {
                        clob = callableStatement.getClob((String) param1);
                    } else if (param1 instanceof Integer) {
                        clob = callableStatement.getClob((Integer) param1);
                    }
                    if (clob != null) {
                        result = new SerialClob(clob);
                    }
                    break;
                }
                case getArray: {
                    Array array = null;
                    if (param1 instanceof String) {
                        array = callableStatement.getArray((String) param1);
                    } else if (param1 instanceof Integer) {
                        array = callableStatement.getArray((Integer) param1);
                    }
                    if (array != null) {
                        result = new SerialArray(array);
                    }
                    break;
                }
                case getURL: {
                    if (param1 instanceof String) {
                        result = callableStatement.getURL((String) param1);
                    } else if (param1 instanceof Integer) {
                        result = callableStatement.getURL((Integer) param1);
                    }
                    break;
                }
                case getNClob: {
                    Clob clob = null;
                    if (param1 instanceof String) {
                        clob = callableStatement.getNClob((String) param1);
                    } else if (param1 instanceof Integer) {
                        clob = callableStatement.getNClob((Integer) param1);
                    }
                    if (clob != null) {
                        result = new SerialNClob(clob);
                    }
                    break;
                }
                case getNString: {
                    if (param1 instanceof String) {
                        result = callableStatement.getNString((String) param1);
                    } else if (param1 instanceof Integer) {
                        result = callableStatement.getNString((Integer) param1);
                    }
                    break;
                }
                case getCharacterStream: {
                    Reader reader = null;
                    if (param1 instanceof String) {
                        reader = callableStatement.getCharacterStream((String) param1);
                    } else if (param1 instanceof Integer) {
                        reader = callableStatement.getCharacterStream((Integer) param1);
                    }
                    if (reader != null) {
                        result = Util.reader2String(reader);
                    }
                    break;
                }
                case getNCharacterStream: {
                    Reader reader = null;
                    if (param1 instanceof String) {
                        reader = callableStatement.getNCharacterStream((String) param1);
                    } else if (param1 instanceof Integer) {
                        reader = callableStatement.getNCharacterStream((Integer) param1);
                    }
                    if (reader != null) {
                        result = Util.reader2String(reader);
                    }
                    break;
                }
                case getRowId: {
                    RowId rowId = null;
                    if (param1 instanceof String) {
                        rowId = callableStatement.getRowId((String) param1);
                    } else if (param1 instanceof Integer) {
                        rowId = callableStatement.getRowId((Integer) param1);
                    }
                    if (rowId != null) {
                        result = new SerialRowId(rowId.getBytes());
                    }
                    break;
                }
            }
        } catch (Exception e) {
            throw new SQLException(e);
        }
        return result;
    }
}
