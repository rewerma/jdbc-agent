package com.jdbcagent.client.pool.hikari;

import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.HikariProxyConnection;

import java.sql.Connection;
import java.sql.SQLException;

public class JAHikariDataSource extends HikariDataSource {
    @Override
    public Connection getConnection() throws SQLException {
        HikariProxyConnection connection = (HikariProxyConnection) super.getConnection();
        return new JAHikariProxyConnection(connection);
    }
}
