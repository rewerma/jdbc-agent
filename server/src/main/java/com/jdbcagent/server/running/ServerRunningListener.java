package com.jdbcagent.server.running;

/**
 * JDBC-Agent server zk 运行监听器
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public interface ServerRunningListener {
    /**
     * 触发现在轮到自己做为active
     */
    void processActiveEnter();

    /**
     * 触发一下当前active模式失败
     */
    void processActiveExit();
}
