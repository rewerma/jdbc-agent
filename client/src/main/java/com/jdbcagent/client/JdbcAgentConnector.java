package com.jdbcagent.client;

import com.jdbcagent.core.protocol.Packet;

import java.sql.SQLException;

/**
 * JDBC-Agent client 连接器接口
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public interface JdbcAgentConnector {
    /**
     * nio的连接
     */
    void connect();

    /**
     * nio断开
     */
    void disconnect();

    /**
     * netty启动
     */
    void start();

    /**
     * netty关闭
     */
    void stop();

    /**
     * 发送数据
     *
     * @param packet 数据包
     * @return 响应数据
     * @throws SQLException
     */
    byte[] write(Packet packet) throws SQLException;

}
