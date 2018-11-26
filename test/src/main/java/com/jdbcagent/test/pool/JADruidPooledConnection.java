package com.jdbcagent.test.pool;

import com.alibaba.druid.pool.DruidConnectionHolder;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.jdbcagent.test.jdbc.JdbcConnection;

import java.sql.SQLException;

public class JADruidPooledConnection extends DruidPooledConnection {
    public JADruidPooledConnection(DruidConnectionHolder holder) {
        super(holder);
    }

    @Override
    public void close() throws SQLException {
//        try {
//            // 释放远程jdbc-agentserver的db connection
//            ((JdbcConnection) conn).release();
//        } catch (Exception e) {
//            // ignore
//        }

        super.close();
    }
}
