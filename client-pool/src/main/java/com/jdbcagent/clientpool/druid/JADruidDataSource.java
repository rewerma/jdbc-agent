package com.jdbcagent.clientpool.druid;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;

import java.sql.SQLException;

public class JADruidDataSource extends DruidDataSource {
    @Override
    public DruidPooledConnection getConnectionDirect(long maxWaitMillis) throws SQLException {
        return new JADruidPooledConnection(super.getConnectionDirect(maxWaitMillis).getConnectionHolder());
    }
}
