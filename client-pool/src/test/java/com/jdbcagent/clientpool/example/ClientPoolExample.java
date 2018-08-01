package com.jdbcagent.clientpool.example;

import com.alibaba.druid.pool.DruidDataSource;
import com.jdbcagent.clientpool.druid.JADruidDataSource;

import java.sql.*;

public class ClientPoolExample {
    public static void main(String[] args) throws SQLException {
        DruidDataSource druidDataSource = null;
        Connection conn = null;
        try {
            druidDataSource = new JADruidDataSource();
            druidDataSource.setDriverClassName("com.jdbcagent.client.jdbc.Driver");
            druidDataSource.setUrl("jdbc:agent:127.0.0.1:10100/mytest");
            druidDataSource.setUsername("test");
            druidDataSource.setPassword("123456");
            druidDataSource.setInitialSize(1);
            druidDataSource.setMinIdle(1);
            druidDataSource.setMaxActive(10);
            druidDataSource.setMaxWait(60000);
            druidDataSource.setTimeBetweenEvictionRunsMillis(60000);
            druidDataSource.setMinEvictableIdleTimeMillis(300000);
            druidDataSource.setMaxPoolPreparedStatementPerConnectionSize(20);
            druidDataSource.setValidationQuery("select 1");

            conn = druidDataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select * from t_user");
            ResultSetMetaData rsMD = rs.getMetaData();
            System.out.println("columns count: " + rsMD.getColumnCount() + ", first column: " + rsMD.getColumnName(1));
            while (rs.next()) {
                System.out.println(rs.getLong("id") + " " + rs.getString("name") + " "
                        + rs.getInt("gender") + " " + rs.getString("email") + " "
                        + rs.getTimestamp("sys_time"));
            }
            rs.close();

            int count = stmt.executeUpdate("update t_user set name='test_0' where id=1");
            System.out.println("update count: " + count);

            stmt.close();

            PreparedStatement pstmt = conn.prepareStatement("select * from t_user where id=?");
            pstmt.setLong(1, 2L);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getLong("id") + " " + rs.getString("name") + " "
                        + rs.getInt("gender") + " " + rs.getString("email") + " "
                        + rs.getTimestamp("sys_time"));
            }
            rs.close();
            pstmt.close();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                conn.close();
            }
            if (druidDataSource != null) {
                druidDataSource.close();
            }
        }
    }
}
