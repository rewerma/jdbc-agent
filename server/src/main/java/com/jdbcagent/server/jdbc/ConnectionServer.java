package com.jdbcagent.server.jdbc;

import com.jdbcagent.core.protocol.ConnectionMsg;
import com.jdbcagent.core.support.serial.SerialConnection;
import com.jdbcagent.core.support.serial.SerialNClob;
import com.jdbcagent.core.support.serial.SerialSavepoint;
import com.jdbcagent.core.support.serial.SerialVoid;
import com.jdbcagent.server.config.JdbcAgentConf;
import org.apache.commons.lang.StringUtils;

import javax.sql.DataSource;
import javax.sql.rowset.serial.SerialArray;
import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import java.io.Serializable;
import java.sql.*;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * JDBC-Agent server 端 connection 操作类
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class ConnectionServer {
    private static AtomicLong CONNECTION_ID = new AtomicLong(0);    // id与client对应

    public static ConcurrentHashMap<Long, ConnectionServer>
            CONNECTIONS = new ConcurrentHashMap<>();                // connectionServer 缓存

    private long currentId = -1;                                    // 当前id

    private Connection connection;                                  // 实际调用的connection

    private Connection writerConn;

    private Connection readerConn;

    private String catalog;                                         // 目录名

    private String username;                                        // 客户端连接用户名

    private String password;                                        // 客户端连接密码

    private SerialConnection serialConnection;                      // 可序列化的conn, 用于保存部分conn信息

    /**
     * 客户端创建与server连接
     *
     * @param username 用户名
     * @param password 密码
     * @return connectionServer的id
     * @throws SQLException
     */
    public long connect(String catalog, String username, String password) throws SQLException {
        this.catalog = catalog;
        this.username = username;
        this.password = password;
        currentId = CONNECTION_ID.incrementAndGet();
        CONNECTIONS.put(currentId, this);

        initConnection();

        return currentId;
    }

    /**
     * 客户端关闭与server连接
     *
     * @throws SQLException
     */
    public void close() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
            connection = null;
        }
        if (writerConn != null && !writerConn.isClosed()) {
            writerConn.close();
            writerConn = null;
        }
        if (readerConn != null && !readerConn.isClosed()) {
            readerConn.close();
            readerConn = null;
        }
        CONNECTIONS.remove(currentId); // 从缓存中清除
    }

    public SerialConnection getSerialConnection() {
        return serialConnection;
    }

    /**
     * 获取DataSource的connection
     */
    private void initConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {


            DataSource writerDataSource = JdbcAgentConf.getWriteDataSource(
                    StringUtils.trimToEmpty(catalog)
                            + "|" + StringUtils.trimToEmpty(username)
                            + "|" + StringUtils.trimToEmpty(password));
            if (writerDataSource != null) {
                this.writerConn = writerDataSource.getConnection();
            }

            DataSource readerDataSource = JdbcAgentConf.getReadDataSource(
                    StringUtils.trimToEmpty(catalog)
                            + "|" + StringUtils.trimToEmpty(username)
                            + "|" + StringUtils.trimToEmpty(password));
            if (readerDataSource != null) {
                this.readerConn = readerDataSource.getConnection();
            }

            DataSource dataSource = JdbcAgentConf.getDataSource(
                    StringUtils.trimToEmpty(catalog)
                            + "|" + StringUtils.trimToEmpty(username)
                            + "|" + StringUtils.trimToEmpty(password));
            if (dataSource == null) {
                throw new SQLException("Error username or password to access. ");
            }

            if (writerDataSource != null && readerDataSource != null) {
                if (readerDataSource == dataSource) {
                    connection = this.readerConn;
                } else {
                    connection = this.writerConn;
                }
            } else {
                connection = dataSource.getConnection();
            }

            serialConnection = new SerialConnection();
            try {
                serialConnection.setAutoCommit(connection.getAutoCommit());
                serialConnection.setCatalog(connection.getCatalog());
                serialConnection.setClientInfo(connection.getClientInfo());
                serialConnection.setHoldability(connection.getHoldability());
                serialConnection.setReadOnly(connection.isReadOnly());
                serialConnection.setTransactionIsolation(connection.getTransactionIsolation());
                serialConnection.setSchema(connection.getSchema());
            } catch (Exception e) {
                //ignore
            }
        }
    }

    public String getWarnings() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            SQLWarning sqlWarning = connection.getWarnings();
            if (sqlWarning != null) {
                return sqlWarning.getMessage();
            }
        }
        return null;
    }

    /**
     * 公共方法调用
     *
     * @param connectMsg 调用信息
     * @return 返回结果
     * @throws SQLException
     */
    public Serializable connMethod(ConnectionMsg connectMsg) throws SQLException {
        try {
            Serializable response = new SerialVoid();
            ConnectionMsg.Method method = connectMsg.getMethod();
            switch (method) {
                case createStatement: {
                    int len = connectMsg.getParams().length;
                    Statement stmt = null;
                    Statement writerStmt = null;
                    Statement readerStmt = null;
                    if (len == 0) {
                        if (writerConn != null && readerConn != null) {
                            writerStmt = writerConn.createStatement();
                            readerStmt = readerConn.createStatement();
                        }
                        if (writerStmt != null && readerStmt != null) {
                            if (connection == readerConn) {
                                stmt = readerStmt;
                            } else {
                                stmt = writerStmt;
                            }
                        }
                        if (stmt == null) {
                            stmt = connection.createStatement();
                        }
                        StatementServer statementServer = new StatementServer(
                                stmt, writerStmt, readerStmt);
                        response = statementServer.currentId;
                    } else if (len == 2) {
                        int resultSetType = (Integer) connectMsg.getParams()[0];
                        int resultSetConcurrency = (Integer) connectMsg.getParams()[1];
                        if (writerConn != null && readerConn != null) {
                            writerStmt = writerConn.createStatement(resultSetType, resultSetConcurrency);
                            readerStmt = readerConn.createStatement(resultSetType, resultSetConcurrency);
                        }
                        if (writerStmt != null && readerStmt != null) {
                            if (connection == readerConn) {
                                stmt = readerStmt;
                            } else {
                                stmt = writerStmt;
                            }
                        }
                        if (stmt == null) {
                            stmt = connection.createStatement(resultSetType, resultSetConcurrency);
                        }
                        StatementServer statementServer =
                                new StatementServer(stmt, writerStmt, readerStmt);
                        response = statementServer.currentId;
                    } else if (len == 3) {
                        int resultSetType = (Integer) connectMsg.getParams()[0];
                        int resultSetConcurrency = (Integer) connectMsg.getParams()[1];
                        int resultSetHoldability = (Integer) connectMsg.getParams()[3];
                        if (writerConn != null && readerConn != null) {
                            writerStmt = writerConn.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
                            readerStmt = readerConn.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
                        }
                        if (writerStmt != null && readerStmt != null) {
                            if (connection == readerConn) {
                                stmt = readerStmt;
                            } else {
                                stmt = writerStmt;
                            }
                        }
                        if (stmt == null) {
                            stmt = connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
                        }
                        StatementServer statementServer =
                                new StatementServer(stmt, writerStmt, readerStmt);
                        response = statementServer.currentId;
                    }
                    break;
                }
                case prepareStatement: {
                    int len = connectMsg.getParams().length;
                    PreparedStatement pstmt = null;
                    PreparedStatement writerPStmt = null;
                    PreparedStatement readerPStmt = null;

                    if (len == 1) {
                        String sql = (String) connectMsg.getParams()[0];

                        if (writerConn != null && readerConn != null) {
                            writerPStmt = writerConn.prepareStatement(sql);
                            readerPStmt = readerConn.prepareStatement(sql);
                        }
                        if (writerPStmt != null && readerPStmt != null) {
                            if (connection == readerConn) {
                                pstmt = readerPStmt;
                            } else {
                                pstmt = writerPStmt;
                            }
                        }

                        if (pstmt == null) {
                            pstmt = connection.prepareStatement(sql);
                        }

                        PreparedStatementServer preparedStatementServer =
                                new PreparedStatementServer(pstmt, writerPStmt, readerPStmt);
                        response = preparedStatementServer.currentId;
                    } else if (len == 2) {
                        String sql = (String) connectMsg.getParams()[0];
                        Object param2 = connectMsg.getParams()[1];
                        if (param2 instanceof Integer) {
                            if (writerConn != null && readerConn != null) {
                                writerPStmt = writerConn.prepareStatement(sql, (Integer) param2);
                                readerPStmt = readerConn.prepareStatement(sql, (Integer) param2);
                            }
                            if (writerPStmt != null && readerPStmt != null) {
                                if (connection == readerConn) {
                                    pstmt = readerPStmt;
                                } else {
                                    pstmt = writerPStmt;
                                }
                            }

                            if (pstmt == null) {
                                pstmt = connection.prepareStatement(sql, (Integer) param2);
                            }
                            PreparedStatementServer preparedStatementServer =
                                    new PreparedStatementServer(pstmt, writerPStmt, readerPStmt);
                            response = preparedStatementServer.currentId;
                        } else if (param2 instanceof int[]) {
                            if (writerConn != null && readerConn != null) {
                                writerPStmt = writerConn.prepareStatement(sql, (int[]) param2);
                                readerPStmt = readerConn.prepareStatement(sql, (int[]) param2);
                            }
                            if (writerPStmt != null && readerPStmt != null) {
                                if (connection == readerConn) {
                                    pstmt = readerPStmt;
                                } else {
                                    pstmt = writerPStmt;
                                }
                            }

                            if (pstmt == null) {
                                pstmt = connection.prepareStatement(sql, (int[]) param2);
                            }
                            PreparedStatementServer preparedStatementServer =
                                    new PreparedStatementServer(pstmt, writerPStmt, readerPStmt);
                            response = preparedStatementServer.currentId;
                        } else if (param2 instanceof String[]) {
                            if (writerConn != null && readerConn != null) {
                                writerPStmt = writerConn.prepareStatement(sql, (String[]) param2);
                                readerPStmt = readerConn.prepareStatement(sql, (String[]) param2);
                            }
                            if (writerPStmt != null && readerPStmt != null) {
                                if (connection == readerConn) {
                                    pstmt = readerPStmt;
                                } else {
                                    pstmt = writerPStmt;
                                }
                            }

                            if (pstmt == null) {
                                pstmt = connection.prepareStatement(sql, (String[]) param2);
                            }
                            PreparedStatementServer preparedStatementServer =
                                    new PreparedStatementServer(pstmt, writerPStmt, readerPStmt);
                            response = preparedStatementServer.currentId;
                        }
                    } else if (len == 3) {
                        String sql = (String) connectMsg.getParams()[0];
                        int resultSetType = (Integer) connectMsg.getParams()[1];
                        int resultSetConcurrency = (Integer) connectMsg.getParams()[2];

                        if (writerConn != null && readerConn != null) {
                            writerPStmt = writerConn.prepareStatement(sql, resultSetType, resultSetConcurrency);
                            readerPStmt = readerConn.prepareStatement(sql, resultSetType, resultSetConcurrency);
                        }
                        if (writerPStmt != null && readerPStmt != null) {
                            if (connection == readerConn) {
                                pstmt = readerPStmt;
                            } else {
                                pstmt = writerPStmt;
                            }
                        }

                        if (pstmt == null) {
                            pstmt = connection.prepareStatement(sql, resultSetType, resultSetConcurrency);
                        }

                        PreparedStatementServer preparedStatementServer =
                                new PreparedStatementServer(pstmt, writerPStmt, readerPStmt);
                        response = preparedStatementServer.currentId;
                    } else if (len == 4) {
                        String sql = (String) connectMsg.getParams()[0];
                        int resultSetType = (Integer) connectMsg.getParams()[1];
                        int resultSetConcurrency = (Integer) connectMsg.getParams()[2];
                        int resultSetHoldability = (Integer) connectMsg.getParams()[3];

                        if (writerConn != null && readerConn != null) {
                            writerPStmt = writerConn.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
                            readerPStmt = readerConn.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
                        }
                        if (writerPStmt != null && readerPStmt != null) {
                            if (connection == readerConn) {
                                pstmt = readerPStmt;
                            } else {
                                pstmt = writerPStmt;
                            }
                        }

                        if (pstmt == null) {
                            pstmt = connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
                        }

                        PreparedStatementServer preparedStatementServer =
                                new PreparedStatementServer(pstmt, writerPStmt, readerPStmt);
                        response = preparedStatementServer.currentId;
                    }
                    break;
                }
                case prepareCall: {
                    int len = connectMsg.getParams().length;
                    CallableStatement cstmt = null;
                    CallableStatement writerCStmt = null;
                    CallableStatement readerCStmt = null;
                    if (len == 1) {
                        String sql = (String) connectMsg.getParams()[0];

                        if (writerConn != null && readerConn != null) {
                            writerCStmt = writerConn.prepareCall(sql);
                            readerCStmt = readerConn.prepareCall(sql);
                        }
                        if (writerCStmt != null && readerCStmt != null) {
                            if (connection == readerConn) {
                                cstmt = readerCStmt;
                            } else {
                                cstmt = writerCStmt;
                            }
                        }

                        if (cstmt == null) {
                            cstmt = connection.prepareCall(sql);
                        }

                        CallableStatementServer callableStatementServer =
                                new CallableStatementServer(cstmt, writerCStmt, readerCStmt);
                        response = callableStatementServer.currentId;
                    } else if (len == 3) {
                        String sql = (String) connectMsg.getParams()[0];
                        int resultSetType = (Integer) connectMsg.getParams()[1];
                        int resultSetConcurrency = (Integer) connectMsg.getParams()[2];

                        if (writerConn != null && readerConn != null) {
                            writerCStmt = writerConn.prepareCall(sql, resultSetType, resultSetConcurrency);
                            readerCStmt = readerConn.prepareCall(sql, resultSetType, resultSetConcurrency);
                        }
                        if (writerCStmt != null && readerCStmt != null) {
                            if (connection == readerConn) {
                                cstmt = readerCStmt;
                            } else {
                                cstmt = writerCStmt;
                            }
                        }

                        if (cstmt == null) {
                            cstmt = connection.prepareCall(sql, resultSetType, resultSetConcurrency);
                        }

                        CallableStatementServer callableStatementServer =
                                new CallableStatementServer(cstmt, writerCStmt, readerCStmt);
                        response = callableStatementServer.currentId;
                    } else if (len == 4) {
                        String sql = (String) connectMsg.getParams()[0];
                        int resultSetType = (Integer) connectMsg.getParams()[1];
                        int resultSetConcurrency = (Integer) connectMsg.getParams()[2];
                        int resultSetHoldability = (Integer) connectMsg.getParams()[3];

                        if (writerConn != null && readerConn != null) {
                            writerCStmt = writerConn.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
                            readerCStmt = readerConn.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
                        }
                        if (writerCStmt != null && readerCStmt != null) {
                            if (connection == readerConn) {
                                cstmt = readerCStmt;
                            } else {
                                cstmt = writerCStmt;
                            }
                        }

                        if (cstmt == null) {
                            cstmt = connection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
                        }

                        CallableStatementServer callableStatementServer =
                                new CallableStatementServer(cstmt, writerCStmt, readerCStmt);
                        response = callableStatementServer.currentId;
                    }
                    break;
                }
                case getMetaData: {
                    DatabaseMetaDataServer databaseMetaDataServer =
                            new DatabaseMetaDataServer(connection.getMetaData());
                    response = databaseMetaDataServer.currentId;
                    break;
                }
                case nativeSQL: {
                    String sql = (String) connectMsg.getParams()[0];
                    response = connection.nativeSQL(sql);
                    break;
                }
                case setAutoCommit: {
                    boolean autoCommit = (Boolean) connectMsg.getParams()[0];
                    connection.setAutoCommit(autoCommit);
                    break;
                }
                case getAutoCommit: {
                    response = connection.getAutoCommit();
                    break;
                }
                case commit: {
                    connection.commit();
                    break;
                }
                case rollback: {
                    int len = connectMsg.getParams().length;
                    if (len == 0) {
                        connection.rollback();
                    } else if (len == 1) {
                        SerialSavepoint savepoint = (SerialSavepoint) connectMsg.getParams()[0];
                        connection.rollback(savepoint);
                    }
                    break;
                }
                case isClosed: {
                    response = connection.isClosed();
                    break;
                }
                case setReadOnly: {
                    boolean readOnly = (Boolean) connectMsg.getParams()[0];
                    connection.setReadOnly(readOnly);
                    break;
                }
                case isReadOnly: {
                    response = connection.isReadOnly();
                    break;
                }
                case setCatalog: {
                    String catalog = (String) connectMsg.getParams()[0];
                    connection.setCatalog(catalog);
                    break;
                }
                case getCatalog: {
                    response = connection.getCatalog();
                    break;
                }
                case setTransactionIsolation: {
                    int level = (Integer) connectMsg.getParams()[0];
                    connection.setTransactionIsolation(level);
                    break;
                }
                case getTransactionIsolation: {
                    response = connection.getTransactionIsolation();
                    break;
                }
                case getWarnings: {
                    if (connection.getWarnings() == null) {
                        response = null;
                    } else {
                        response = connection.getWarnings().getMessage();
                    }
                    break;
                }
                case clearWarnings: {
                    connection.clearWarnings();
                    break;
                }
                case setHoldability: {
                    int holdability = (Integer) connectMsg.getParams()[0];
                    connection.setHoldability(holdability);
                    break;
                }
                case getHoldability: {
                    response = connection.getHoldability();
                    break;
                }
                case setSavepoint: {
                    Savepoint savepoint;
                    if (connectMsg.getParams().length == 0) {
                        savepoint = connection.setSavepoint();
                    } else {
                        String name = (String) connectMsg.getParams()[0];
                        savepoint = connection.setSavepoint(name);
                    }
                    response = new SerialSavepoint(savepoint.getSavepointId(),
                            savepoint.getSavepointName());
                    break;
                }
                case releaseSavepoint: {
                    SerialSavepoint savepoint = (SerialSavepoint) connectMsg.getParams()[0];
                    connection.releaseSavepoint(savepoint);
                    break;
                }
                case createClob: {
                    Clob clob = connection.createClob();
                    if (clob != null) {
                        response = new SerialClob(clob);
                    }
                    break;
                }
                case createBlob: {
                    Blob blob = connection.createBlob();
                    if (blob != null) {
                        response = new SerialBlob(blob);
                    }
                    break;
                }
                case createNClob: {
                    Clob clob = connection.createNClob();
                    if (clob != null) {
                        response = new SerialNClob(clob);
                    }
                    break;
                }
                case isValid: {
                    int timeout = (Integer) connectMsg.getParams()[0];
                    response = connection.isValid(timeout);
                    break;
                }
                case setClientInfo: {
                    int len = connectMsg.getParams().length;
                    if (len == 1) {
                        Properties properties = (Properties) connectMsg.getParams()[0];
                        connection.setClientInfo(properties);
                    } else if (len == 2) {
                        String name = (String) connectMsg.getParams()[0];
                        String value = (String) connectMsg.getParams()[1];
                        connection.setClientInfo(name, value);
                    }
                    break;
                }
                case getClientInfo: {
                    if (connectMsg.getParams().length == 0) {
                        response = connection.getClientInfo();
                    } else {
                        String name = (String) connectMsg.getParams()[0];
                        response = connection.getClientInfo(name);
                    }
                    break;
                }
                case createArrayOf: {
                    String typeName = (String) connectMsg.getParams()[0];
                    Object[] elements = (Object[]) connectMsg.getParams()[1];
                    response = new SerialArray(connection.createArrayOf(typeName, elements));
                    break;
                }
                case setSchema: {
                    String schema = (String) connectMsg.getParams()[0];
                    connection.setSchema(schema);
                    break;
                }
                case getNetworkTimeout: {
                    response = connection.getNetworkTimeout();
                    break;
                }
                // 附加方法，用于释放与数据库的连接而不断开与客户端的连接
                case release: {
                    if (connection != null && !connection.isClosed()) {
                        connection.clearWarnings();
                        connection.close();
                        connection = null;
                    }
                    break;
                }
                default: {
                    break;
                }
            }
            return response;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }
}
