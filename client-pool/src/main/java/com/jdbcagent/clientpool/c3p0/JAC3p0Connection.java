package com.jdbcagent.clientpool.c3p0;

import com.jdbcagent.client.jdbc.JdbcConnection;
import com.jdbcagent.clientpool.ProxyConnection;
import com.mchange.v2.c3p0.impl.NewProxyConnection;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;

public class JAC3p0Connection extends ProxyConnection implements Connection {
    public JAC3p0Connection(Connection conn) {
        super(conn);
    }

    @Override
    public void close() throws SQLException {
        JdbcConnection jdbcConn = null;
        if (conn instanceof NewProxyConnection) {
            NewProxyConnection newProxyConn = (NewProxyConnection) conn;
            try {
                Field innerField = NewProxyConnection.class.getDeclaredField("inner");
                innerField.setAccessible(true);
                jdbcConn = (JdbcConnection) innerField.get(newProxyConn);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        conn.close();
        if (jdbcConn != null) {
            jdbcConn.release();
        }

    }
}
