package com.jdbcagent.server;

/**
 * JDBC-Agent server interface
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public interface JdbcAgentServer {
    /**
     * 启动
     */
    void start();

    /**
     * 关闭
     */
    void stop();

    /**
     * 是否启动
     *
     * @return
     */
    boolean isStart();
}
