package com.jdbcagent.client.jdbc;

import com.jdbcagent.core.protocol.Packet;

import java.sql.SQLException;

/**
 * JDBC-Agent client 连接器接口
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public abstract class JdbcAgentConnector {
    /**
     * nio的连接
     */
    public void connect() {
    }

    /**
     * nio断开
     */
    public void disconnect() {
    }

    /**
     * netty启动
     */
    public void start() {

    }

    /**
     * netty关闭
     */
    public void stop() {

    }

    /**
     * 发送数据
     *
     * @param packet 数据包
     * @return 响应数据
     * @throws SQLException
     */
    public abstract byte[] write(Packet packet) throws SQLException;

}
