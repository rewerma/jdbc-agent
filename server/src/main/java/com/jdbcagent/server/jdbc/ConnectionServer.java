package com.jdbcagent.server.jdbc;

import com.alibaba.druid.pool.DruidDataSource;
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
        CONNECTIONS.remove(currentId); // 从缓存中清除
    }

    public SerialConnection getSerialConnection() {
        return serialConnection;
    }

    /**
     * 获取DataSource的connection
     * 延迟创建connection即nio连接进来后不立即创建db的conn
     */
    private Connection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            DataSource dataSource = JdbcAgentConf.getDataSource(
                    StringUtils.trimToEmpty(catalog)
                            + "|" + StringUtils.trimToEmpty(username)
                            + "|" + StringUtils.trimToEmpty(password));
            if (dataSource == null) {
                throw new SQLException("Error username or password to access. ");
            }

            if (dataSource instanceof DruidDataSource && ((DruidDataSource) dataSource).isClosed()) {
                throw new SQLException("dataSource already closed");
            }


            connection = dataSource.getConnection();

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
        return connection;
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
                    if (len == 0) {
                        StatementServer statementServer = new StatementServer(
                                getConnection().createStatement());
                        response = statementServer.currentId;
                    } else if (len == 2) {
                        int resultSetType = (Integer) connectMsg.getParams()[0];
                        int resultSetConcurrency = (Integer) connectMsg.getParams()[1];
                        StatementServer statementServer =
                                new StatementServer(getConnection().createStatement(resultSetType, resultSetConcurrency));
                        response = statementServer.currentId;
                    } else if (len == 3) {
                        int resultSetType = (Integer) connectMsg.getParams()[0];
                        int resultSetConcurrency = (Integer) connectMsg.getParams()[1];
                        int resultSetHoldability = (Integer) connectMsg.getParams()[3];
                        StatementServer statementServer =
                                new StatementServer(getConnection().createStatement(resultSetType, resultSetConcurrency, resultSetHoldability));
                        response = statementServer.currentId;
                    }
                    break;
                }
                case prepareStatement: {
                    int len = connectMsg.getParams().length;
                    if (len == 1) {
                        String sql = (String) connectMsg.getParams()[0];
                        PreparedStatementServer preparedStatementServer =
                                new PreparedStatementServer(getConnection()
                                        .prepareStatement(sql));
                        response = preparedStatementServer.currentId;
                    } else if (len == 2) {
                        String sql = (String) connectMsg.getParams()[0];
                        Object param2 = connectMsg.getParams()[1];
                        if (param2 instanceof Integer) {
                            PreparedStatementServer preparedStatementServer =
                                    new PreparedStatementServer(getConnection().prepareStatement(sql, (Integer) param2));
                            response = preparedStatementServer.currentId;
                        } else if (param2 instanceof int[]) {
                            PreparedStatementServer preparedStatementServer =
                                    new PreparedStatementServer(getConnection().prepareStatement(sql, (int[]) param2));
                            response = preparedStatementServer.currentId;
                        } else if (param2 instanceof String[]) {
                            PreparedStatementServer preparedStatementServer =
                                    new PreparedStatementServer(getConnection().prepareStatement(sql, (String[]) param2));
                            response = preparedStatementServer.currentId;
                        }
                    } else if (len == 3) {
                        String sql = (String) connectMsg.getParams()[0];
                        int resultSetType = (Integer) connectMsg.getParams()[1];
                        int resultSetConcurrency = (Integer) connectMsg.getParams()[2];
                        PreparedStatementServer preparedStatementServer =
                                new PreparedStatementServer(getConnection().prepareStatement(sql,
                                        resultSetType, resultSetConcurrency));
                        response = preparedStatementServer.currentId;
                    } else if (len == 4) {
                        String sql = (String) connectMsg.getParams()[0];
                        int resultSetType = (Integer) connectMsg.getParams()[1];
                        int resultSetConcurrency = (Integer) connectMsg.getParams()[2];
                        int resultSetHoldability = (Integer) connectMsg.getParams()[3];
                        PreparedStatementServer preparedStatementServer =
                                new PreparedStatementServer(getConnection().prepareStatement(sql,
                                        resultSetType, resultSetConcurrency, resultSetHoldability));
                        response = preparedStatementServer.currentId;
                    }
                    break;
                }
                case prepareCall: {
                    int len = connectMsg.getParams().length;
                    if (len == 1) {
                        String sql = (String) connectMsg.getParams()[0];
                        CallableStatementServer callableStatementServer =
                                new CallableStatementServer(getConnection()
                                        .prepareCall(sql));
                        response = callableStatementServer.currentId;
                    } else if (len == 3) {
                        String sql = (String) connectMsg.getParams()[0];
                        int resultSetType = (Integer) connectMsg.getParams()[1];
                        int resultSetConcurrency = (Integer) connectMsg.getParams()[2];
                        CallableStatementServer callableStatementServer =
                                new CallableStatementServer(getConnection().prepareCall(sql,
                                        resultSetType, resultSetConcurrency));
                        response = callableStatementServer.currentId;
                    } else if (len == 4) {
                        String sql = (String) connectMsg.getParams()[0];
                        int resultSetType = (Integer) connectMsg.getParams()[1];
                        int resultSetConcurrency = (Integer) connectMsg.getParams()[2];
                        int resultSetHoldability = (Integer) connectMsg.getParams()[3];
                        CallableStatementServer callableStatementServer =
                                new CallableStatementServer(getConnection().prepareCall(sql,
                                        resultSetType, resultSetConcurrency, resultSetHoldability));
                        response = callableStatementServer.currentId;
                    }
                    break;
                }
                case getMetaData: {
                    DatabaseMetaDataServer databaseMetaDataServer =
                            new DatabaseMetaDataServer(getConnection().getMetaData());
                    response = databaseMetaDataServer.currentId;
                    break;
                }
                case nativeSQL: {
                    String sql = (String) connectMsg.getParams()[0];
                    response = getConnection().nativeSQL(sql);
                    break;
                }
                case setAutoCommit: {
                    boolean autoCommit = (Boolean) connectMsg.getParams()[0];
                    getConnection().setAutoCommit(autoCommit);
                    break;
                }
                case getAutoCommit: {
                    response = getConnection().getAutoCommit();
                    break;
                }
                case commit: {
                    getConnection().commit();
                    break;
                }
                case rollback: {
                    int len = connectMsg.getParams().length;
                    if (len == 0) {
                        getConnection().rollback();
                    } else if (len == 1) {
                        SerialSavepoint savepoint = (SerialSavepoint) connectMsg.getParams()[0];
                        getConnection().rollback(savepoint);
                    }
                    break;
                }
                case isClosed: {
                    response = getConnection().isClosed();
                    break;
                }
                case setReadOnly: {
                    boolean readOnly = (Boolean) connectMsg.getParams()[0];
                    getConnection().setReadOnly(readOnly);
                    break;
                }
                case isReadOnly: {
                    response = getConnection().isReadOnly();
                    break;
                }
                case setCatalog: {
                    String catalog = (String) connectMsg.getParams()[0];
                    getConnection().setCatalog(catalog);
                    break;
                }
                case getCatalog: {
                    response = getConnection().getCatalog();
                    break;
                }
                case setTransactionIsolation: {
                    int level = (Integer) connectMsg.getParams()[0];
                    getConnection().setTransactionIsolation(level);
                    break;
                }
                case getTransactionIsolation: {
                    response = getConnection().getTransactionIsolation();
                    break;
                }
                case getWarnings: {
                    if (getConnection().getWarnings() == null) {
                        response = null;
                    } else {
                        response = getConnection().getWarnings().getMessage();
                    }
                    break;
                }
                case clearWarnings: {
                    getConnection().clearWarnings();
                    break;
                }
                case setHoldability: {
                    int holdability = (Integer) connectMsg.getParams()[0];
                    getConnection().setHoldability(holdability);
                    break;
                }
                case getHoldability: {
                    response = getConnection().getHoldability();
                    break;
                }
                case setSavepoint: {
                    Savepoint savepoint;
                    if (connectMsg.getParams().length == 0) {
                        savepoint = getConnection().setSavepoint();
                    } else {
                        String name = (String) connectMsg.getParams()[0];
                        savepoint = getConnection().setSavepoint(name);
                    }
                    response = new SerialSavepoint(savepoint.getSavepointId(),
                            savepoint.getSavepointName());
                    break;
                }
                case releaseSavepoint: {
                    SerialSavepoint savepoint = (SerialSavepoint) connectMsg.getParams()[0];
                    getConnection().releaseSavepoint(savepoint);
                    break;
                }
                case createClob: {
                    Clob clob = getConnection().createClob();
                    if (clob != null) {
                        response = new SerialClob(clob);
                    }
                    break;
                }
                case createBlob: {
                    Blob blob = getConnection().createBlob();
                    if (blob != null) {
                        response = new SerialBlob(blob);
                    }
                    break;
                }
                case createNClob: {
                    Clob clob = getConnection().createNClob();
                    if (clob != null) {
                        response = new SerialNClob(clob);
                    }
                    break;
                }
                case isValid: {
                    int timeout = (Integer) connectMsg.getParams()[0];
                    response = getConnection().isValid(timeout);
                    break;
                }
                case setClientInfo: {
                    int len = connectMsg.getParams().length;
                    if (len == 1) {
                        Properties properties = (Properties) connectMsg.getParams()[0];
                        getConnection().setClientInfo(properties);
                    } else if (len == 2) {
                        String name = (String) connectMsg.getParams()[0];
                        String value = (String) connectMsg.getParams()[1];
                        getConnection().setClientInfo(name, value);
                    }
                    break;
                }
                case getClientInfo: {
                    if (connectMsg.getParams().length == 0) {
                        response = getConnection().getClientInfo();
                    } else {
                        String name = (String) connectMsg.getParams()[0];
                        response = getConnection().getClientInfo(name);
                    }
                    break;
                }
                case createArrayOf: {
                    String typeName = (String) connectMsg.getParams()[0];
                    Object[] elements = (Object[]) connectMsg.getParams()[1];
                    response = new SerialArray(getConnection().createArrayOf(typeName, elements));
                    break;
                }
                case setSchema: {
                    String schema = (String) connectMsg.getParams()[0];
                    getConnection().setSchema(schema);
                    break;
                }
                case getNetworkTimeout: {
                    response = getConnection().getNetworkTimeout();
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
