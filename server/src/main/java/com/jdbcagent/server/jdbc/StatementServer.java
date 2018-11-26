package com.jdbcagent.server.jdbc;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.jdbcagent.core.protocol.StatementMsg;
import com.jdbcagent.core.support.serial.SerialVoid;

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
    private static AtomicLong STATEMENTS_ID = new AtomicLong(0);    // id与client对应

    public static Cache<Long, StatementServer> STATEMENTS = Caffeine.newBuilder()
            .expireAfterAccess(60, TimeUnit.MINUTES)
            .maximumSize(1000000)
            .build();                                               // statementServer 缓存, 保存60分钟自动删除

    long currentId;                                                 // 当前id

    private Statement statement;                                    // 实际调用的statement

    private Statement writerStmt;

    private Statement readerStmt;

    /**
     * 构造方法
     *
     * @param statement
     */
    StatementServer(Statement statement, Statement writerStmt, Statement readerStmt) {
        currentId = STATEMENTS_ID.incrementAndGet();
        this.statement = statement;
        this.writerStmt = writerStmt;
        this.readerStmt = readerStmt;
        STATEMENTS.put(currentId, this);
    }

    /**
     * 关闭方法
     *
     * @throws SQLException
     */
    public void close() throws SQLException {
        try {
            close(statement);
            close(writerStmt);
            close(readerStmt);
            STATEMENTS.invalidate(currentId);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    protected void close(Statement statement) throws SQLException {
        if (statement != null && !statement.isClosed()) {
            statement.close();
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
                    Statement stmt = statement;
                    if (readerStmt != null) {
                        stmt = readerStmt;
                    }
                    String sql = (String) statementMsg.getParams()[0];
                    ResultSetServer resultSetServer =
                            new ResultSetServer(stmt.executeQuery(sql));
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

                    if (writerStmt != null) {
                        writerStmt.setMaxFieldSize(max);
                    }
                    if (readerStmt != null) {
                        readerStmt.setMaxFieldSize(max);
                    }
                    break;
                }
                case getMaxRows: {
                    response = statement.getMaxRows();
                    break;
                }
                case setMaxRows: {
                    int max = (Integer) statementMsg.getParams()[0];
                    statement.setMaxRows(max);

                    if (writerStmt != null) {
                        writerStmt.setMaxRows(max);
                    }
                    if (readerStmt != null) {
                        readerStmt.setMaxRows(max);
                    }
                    break;
                }
                case setEscapeProcessing: {
                    boolean enable = (Boolean) statementMsg.getParams()[0];
                    statement.setEscapeProcessing(enable);

                    if (writerStmt != null) {
                        writerStmt.setEscapeProcessing(enable);
                    }
                    if (readerStmt != null) {
                        readerStmt.setEscapeProcessing(enable);
                    }
                    break;
                }
                case getQueryTimeout: {
                    response = statement.getQueryTimeout();
                    break;
                }
                case setQueryTimeout: {
                    int seconds = (Integer) statementMsg.getParams()[0];
                    statement.setQueryTimeout(seconds);

                    if (writerStmt != null) {
                        writerStmt.setQueryTimeout(seconds);
                    }
                    if (readerStmt != null) {
                        readerStmt.setQueryTimeout(seconds);
                    }
                    break;
                }
                case cancel: {
                    statement.cancel();

                    if (writerStmt != null) {
                        writerStmt.cancel();
                    }
                    if (readerStmt != null) {
                        readerStmt.cancel();
                    }
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

                    if (writerStmt != null) {
                        writerStmt.clearWarnings();
                    }
                    if (readerStmt != null) {
                        readerStmt.clearWarnings();
                    }
                    break;
                }
                case setCursorName: {
                    String name = (String) statementMsg.getParams()[0];
                    statement.setCursorName(name);

                    if (writerStmt != null) {
                        writerStmt.setCursorName(name);
                    }
                    if (readerStmt != null) {
                        readerStmt.setCursorName(name);
                    }
                    break;
                }
                case execute: {
                    Statement stmt = statement;
                    if (writerStmt != null) {
                        stmt = writerStmt;
                    }
                    int len = statementMsg.getParams().length;
                    if (len == 1) {
                        String sql = (String) statementMsg.getParams()[0];
                        response = stmt.execute(sql);
                    } else if (len == 2) {
                        String sql = (String) statementMsg.getParams()[0];
                        Object param2 = statementMsg.getParams()[1];
                        if (param2 instanceof Integer) {
                            response = stmt.execute(sql, (Integer) param2);
                        } else if (param2 instanceof int[]) {
                            response = stmt.execute(sql, (int[]) param2);
                        } else if (param2 instanceof String[]) {
                            response = stmt.execute(sql, (String[]) param2);
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

                    if (writerStmt != null) {
                        writerStmt.setFetchDirection(direction);
                    }
                    if (readerStmt != null) {
                        readerStmt.setFetchDirection(direction);
                    }
                    break;
                }
                case getFetchDirection: {
                    response = statement.getFetchDirection();
                    break;
                }
                case setFetchSize: {
                    int rows = (Integer) statementMsg.getParams()[0];
                    statement.setFetchSize(rows);

                    if (writerStmt != null) {
                        writerStmt.setFetchSize(rows);
                    }
                    if (readerStmt != null) {
                        readerStmt.setFetchSize(rows);
                    }
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
                    Statement stmt = statement;
                    if (writerStmt != null) {
                        stmt = writerStmt;
                    }
                    String sql = (String) statementMsg.getParams()[0];
                    stmt.addBatch(sql);
                    break;
                }
                case clearBatch: {
                    Statement stmt = statement;
                    if (writerStmt != null) {
                        stmt = writerStmt;
                    }
                    stmt.clearBatch();
                    break;
                }
                case executeBatch: {
                    Statement stmt = statement;
                    if (writerStmt != null) {
                        stmt = writerStmt;
                    }
                    response = stmt.executeBatch();
                    break;
                }
                case executeUpdate: {
                    Statement stmt = statement;
                    if (writerStmt != null) {
                        stmt = writerStmt;
                    }
                    int len = statementMsg.getParams().length;
                    if (len == 1) {
                        String sql = (String) statementMsg.getParams()[0];
                        response = stmt.executeUpdate(sql);
                    } else if (len == 2) {
                        String sql = (String) statementMsg.getParams()[0];
                        Object param2 = statementMsg.getParams()[1];
                        if (param2 instanceof Integer) {
                            response = stmt.executeUpdate(sql, (Integer) param2);
                        } else if (param2 instanceof int[]) {
                            response = stmt.executeUpdate(sql, (int[]) param2);
                        } else if (param2 instanceof String[]) {
                            response = stmt.executeUpdate(sql, (String[]) param2);
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
                    if (writerStmt != null) {
                        writerStmt.setPoolable(poolable);
                    }
                    if (readerStmt != null) {
                        readerStmt.setPoolable(poolable);
                    }
                    break;
                }
                case isPoolable: {
                    response = statement.isPoolable();
                    break;
                }
                case closeOnCompletion: {
                    statement.closeOnCompletion();
                    if (writerStmt != null) {
                        writerStmt.closeOnCompletion();
                    }
                    if (readerStmt != null) {
                        readerStmt.closeOnCompletion();
                    }
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
