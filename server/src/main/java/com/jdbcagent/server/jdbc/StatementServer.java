package com.jdbcagent.server.jdbc;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.jdbcagent.core.protocol.StatementMsg;
import com.jdbcagent.core.support.serial.SerialVoid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * JDBC-Agent server 端 statement 操作类
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class StatementServer {
    private static final Logger logger = LoggerFactory.getLogger(StatementServer.class);

    private static AtomicLong STATEMENTS_ID = new AtomicLong(0);    // id与client对应

    static final RemovalListener<Long, StatementServer> CLOSE_ON_REMOVAL =
            new RemovalListener<Long, StatementServer>() {
                @Override
                public void onRemoval(RemovalNotification<Long, StatementServer> notification) {
                    StatementServer statementServer = notification.getValue();
                    if (statementServer != null) {
                        statementServer.closeQuietly();
                    }
                }
            };

    public static Cache<Long, StatementServer> STATEMENTS =         // statement 缓存, 60分钟未访问后关闭并移除
            CacheBuilder.newBuilder()
                    .initialCapacity(50)
                    .concurrencyLevel(100)
                    .expireAfterAccess(60, TimeUnit.MINUTES)
                    .removalListener(CLOSE_ON_REMOVAL)
                    .build();

    static {
        ServerCacheMaintenance.start();
    }

    long currentId;                                                 // 当前id

    private Statement statement;                                    // 实际调用的statement

    /**
     * 构造方法
     *
     * @param statement
     */
    StatementServer(Statement statement) {
        currentId = STATEMENTS_ID.incrementAndGet();
        this.statement = statement;
        STATEMENTS.put(currentId, this);
    }

    /**
     * 关闭方法
     *
     * @throws SQLException
     */
    public void close() throws SQLException {
        try {
            closeStatement();
            STATEMENTS.invalidate(currentId);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    private void closeStatement() throws SQLException {
        Statement toClose;
        synchronized (this) {
            toClose = statement;
            statement = null;
        }
        if (toClose == null) {
            return;
        }
        try {
            if (!toClose.isClosed()) {
                toClose.close();
            }
        } catch (SQLException e) {
            synchronized (this) {
                if (statement == null) {
                    statement = toClose;
                }
            }
            throw e;
        }
    }

    private void closeQuietly() {
        try {
            closeStatement();
        } catch (Exception e) {
            logger.warn("close statement failed, id=" + currentId, e);
        }
    }

    /**
     * statement 公共方法调用
     *
     * @param statementMsg 调用信息
     * @return 返回结果
     * @throws SQLException
     */
    public Serializable statementMethod(StatementMsg statementMsg) throws SQLException {
        try {
            Serializable response = new SerialVoid();
            StatementMsg.Method method = statementMsg.getMethod();
            switch (method) {
                case executeQuery: {
                    String sql = (String) statementMsg.getParams()[0];
                    ResultSetServer resultSetServer =
                            new ResultSetServer(statement.executeQuery(sql));
                    response = resultSetServer.currentId;
                    break;
                }
                case getMaxFieldSize: {
                    response = statement.getMaxFieldSize();
                    break;
                }
                case setMaxFieldSize: {
                    int max = (Integer) statementMsg.getParams()[0];
                    statement.setMaxFieldSize(max);
                    break;
                }
                case getMaxRows: {
                    response = statement.getMaxRows();
                    break;
                }
                case setMaxRows: {
                    int max = (Integer) statementMsg.getParams()[0];
                    statement.setMaxRows(max);
                    break;
                }
                case setEscapeProcessing: {
                    boolean enable = (Boolean) statementMsg.getParams()[0];
                    statement.setEscapeProcessing(enable);
                    break;
                }
                case getQueryTimeout: {
                    response = statement.getQueryTimeout();
                    break;
                }
                case setQueryTimeout: {
                    int seconds = (Integer) statementMsg.getParams()[0];
                    statement.setQueryTimeout(seconds);
                    break;
                }
                case cancel: {
                    statement.cancel();
                    break;
                }
                case getWarnings: {
                    SQLWarning sqlWarning = statement.getWarnings();
                    if (sqlWarning != null) {
                        response = sqlWarning.getMessage();
                    } else {
                        response = null;
                    }
                    break;
                }
                case clearWarnings: {
                    statement.clearWarnings();
                    break;
                }
                case setCursorName: {
                    String name = (String) statementMsg.getParams()[0];
                    statement.setCursorName(name);
                    break;
                }
                case execute: {
                    int len = statementMsg.getParams().length;
                    if (len == 1) {
                        String sql = (String) statementMsg.getParams()[0];
                        response = statement.execute(sql);
                    } else if (len == 2) {
                        String sql = (String) statementMsg.getParams()[0];
                        Object param2 = statementMsg.getParams()[1];
                        if (param2 instanceof Integer) {
                            response = statement.execute(sql, (Integer) param2);
                        } else if (param2 instanceof int[]) {
                            response = statement.execute(sql, (int[]) param2);
                        } else if (param2 instanceof String[]) {
                            response = statement.execute(sql, (String[]) param2);
                        }
                    }
                    break;
                }
                case getUpdateCount: {
                    response = statement.getUpdateCount();
                    break;
                }
                case getMoreResults: {
                    int len = statementMsg.getParams().length;
                    if (len == 0) {
                        response = statement.getMoreResults();
                    } else if (len == 1) {
                        int current = (Integer) statementMsg.getParams()[0];
                        response = statement.getMoreResults(current);
                    }
                    break;
                }
                case setFetchDirection: {
                    int direction = (Integer) statementMsg.getParams()[0];
                    statement.setFetchDirection(direction);
                    break;
                }
                case getFetchDirection: {
                    response = statement.getFetchDirection();
                    break;
                }
                case setFetchSize: {
                    int rows = (Integer) statementMsg.getParams()[0];
                    statement.setFetchSize(rows);
                    break;
                }
                case getFetchSize: {
                    response = statement.getFetchSize();
                    break;
                }
                case getResultSetConcurrency: {
                    response = statement.getResultSetConcurrency();
                    break;
                }
                case getResultSetType: {
                    response = statement.getResultSetType();
                    break;
                }
                case addBatch: {
                    String sql = (String) statementMsg.getParams()[0];
                    statement.addBatch(sql);
                    break;
                }
                case clearBatch: {
                    statement.clearBatch();
                    break;
                }
                case executeBatch: {
                    response = statement.executeBatch();
                    break;
                }
                case executeUpdate: {
                    int len = statementMsg.getParams().length;
                    if (len == 1) {
                        String sql = (String) statementMsg.getParams()[0];
                        response = statement.executeUpdate(sql);
                    } else if (len == 2) {
                        String sql = (String) statementMsg.getParams()[0];
                        Object param2 = statementMsg.getParams()[1];
                        if (param2 instanceof Integer) {
                            response = statement.executeUpdate(sql, (Integer) param2);
                        } else if (param2 instanceof int[]) {
                            response = statement.executeUpdate(sql, (int[]) param2);
                        } else if (param2 instanceof String[]) {
                            response = statement.executeUpdate(sql, (String[]) param2);
                        }
                    }
                    break;
                }
                case getResultSetHoldability: {
                    response = statement.getResultSetHoldability();
                    break;
                }
                case isClosed: {
                    response = statement.isClosed();
                    break;
                }
                case setPoolable: {
                    boolean poolable = (Boolean) statementMsg.getParams()[0];
                    statement.setPoolable(poolable);
                    break;
                }
                case isPoolable: {
                    response = statement.isPoolable();
                    break;
                }
                case closeOnCompletion: {
                    statement.closeOnCompletion();
                    break;
                }
                case isCloseOnCompletion: {
                    response = statement.isCloseOnCompletion();
                    break;
                }
            }
            return response;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }
}
