package com.jdbcagent.client.example;

import com.alibaba.druid.pool.DruidDataSource;
import com.jdbcagent.client.jdbc.JdbcConnection;
import com.jdbcagent.client.pool.druid.JADruidDataSource;

import java.sql.*;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ClientPoolExample {
    public static void main(String[] args) throws SQLException {
        JADruidDataSource druidDataSource = null;
        Connection conn = null;
        try {
            druidDataSource = new JADruidDataSource();
            druidDataSource.setDriverClassName("com.jdbcagent.client.jdbc.Driver");
            druidDataSource.setUrl("jdbc:agent:127.0.0.1:10101/mytest");
            druidDataSource.setUsername("test");
            druidDataSource.setPassword("123456");
//            druidDataSource.setDriverClassName("com.mysql.jdbc.Driver");
//            druidDataSource.setUrl("jdbc:mysql://127.0.0.1:3306/mytest");
//            druidDataSource.setUsername("root");
//            druidDataSource.setPassword("121212");
            druidDataSource.setInitialSize(1);
            druidDataSource.setMinIdle(1);
            druidDataSource.setMaxActive(100);
            druidDataSource.setMaxWait(60000);
            druidDataSource.setTimeBetweenEvictionRunsMillis(60000);
            druidDataSource.setMinEvictableIdleTimeMillis(300000);
            druidDataSource.setMaxPoolPreparedStatementPerConnectionSize(20);
            druidDataSource.setValidationQuery("select 1");

            final JADruidDataSource druidDataSource1 = druidDataSource;

            ExecutorService executorService = Executors.newFixedThreadPool(5);

            for (int i = 0; i < 5; i++) {
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        test(druidDataSource1);
//                        test01();
                    }
                });
            }

            executorService.shutdown();
            while (!executorService.isTerminated()) {
                Thread.sleep(100);
            }

//            conn = druidDataSource.getConnection();
//            Statement stmt = conn.createStatement();
//            ResultSet rs = stmt.executeQuery("select * from t_user");
//            ResultSetMetaData rsMD = rs.getMetaData();
//            System.out.println("columns count: " + rsMD.getColumnCount() + ", first column: " + rsMD.getColumnName(1));
//            while (rs.next()) {
//                System.out.println(rs.getLong("id") + " " + rs.getString("name") + " "
//                        + rs.getInt("gender") + " " + rs.getString("email") + " "
//                        + rs.getTimestamp("sys_time"));
//            }
//            rs.close();
//
//            int count = stmt.executeUpdate("update t_user set name='test_0' where id=1");
//            System.out.println("update count: " + count);
//
//            stmt.close();
//
//            PreparedStatement pstmt = conn.prepareStatement("select * from t_user where id=?");
//            pstmt.setLong(1, 2L);
//            rs = pstmt.executeQuery();
//            while (rs.next()) {
//                System.out.println(rs.getLong("id") + " " + rs.getString("name") + " "
//                        + rs.getInt("gender") + " " + rs.getString("email") + " "
//                        + rs.getTimestamp("sys_time"));
//            }
//            rs.close();
//            pstmt.close();

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

    public static void test01() {
        JdbcConnection conn = null;
        try {
            String URL = "jdbc:agent:127.0.0.1:10101/mytest";
            String USER = "test";
            String PASSWORD = "123456";
            Class.forName("com.jdbcagent.client.jdbc.Driver");

            Properties info = new Properties();
            info.put("user", USER);
            info.put("password", PASSWORD);
            info.put("timeout", "600000");

            conn = (JdbcConnection) DriverManager.getConnection(URL, info);

            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select * from t_user");

            System.out.println(conn.getRemoteId());
            ResultSetMetaData rsMD = rs.getMetaData();
            System.out.println("columns count: " + rsMD.getColumnCount() + ", first column: " + rsMD.getColumnName(1));
            while (rs.next()) {
                System.out.println(rs.getLong("id") + " " + rs.getString("name") + " "
                        + rs.getInt("gender") + " " + rs.getString("email") + " "
                        + rs.getTimestamp("sys_time"));
            }
            rs.close();
            stmt.close();

            System.out.println("kkkk");
            PreparedStatement pstmt2 = conn.prepareStatement("select * from t_user where id=?");

            pstmt2.setLong(1, 2L);
            rs = pstmt2.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getLong("id") + " " + rs.getString("name") + " "
                        + rs.getInt("gender") + " " + rs.getString("email") + " "
                        + rs.getTimestamp("sys_time"));
            }
            rs.close();
            pstmt2.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    //ignore
                }
                conn = null;
            }
        }
    }

    public static void test(DruidDataSource druidDataSource) {
        Connection conn = null;
        try {
            conn = druidDataSource.getConnection();

//            Statement stmt = conn.createStatement();
//            ResultSet rs = stmt.executeQuery("select * from t_user");
//            ResultSetMetaData rsMD = rs.getMetaData();
//            System.out.println("columns count: " + rsMD.getColumnCount() + ", first column: " + rsMD.getColumnName(1));
//            while (rs.next()) {
//                System.out.println(rs.getLong("id") + " " + rs.getString("name") + " "
//                        + rs.getInt("gender") + " " + rs.getString("email") + " "
//                        + rs.getTimestamp("sys_time"));
//            }
//            rs.close();
//            stmt.close();

//            DruidPooledPreparedStatement pstmt = (DruidPooledPreparedStatement) conn.prepareStatement("update t_user set name=? where id=?");
//            pstmt.setObject(1, "test_23");
//            pstmt.setObject(2, 2L);
//            int count = pstmt.executeUpdate();
//            System.out.println("update count: " + count);
//
//            pstmt.close();

            PreparedStatement pstmt2 = conn.prepareStatement("select * from t_user where id=?");
            pstmt2.setLong(1, 2L);
            ResultSet rs = pstmt2.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getLong("id") + " " + rs.getString("name") + " "
                        + rs.getInt("gender") + " " + rs.getString("email") + " "
                        + rs.getTimestamp("sys_time"));
            }
            rs.close();
            pstmt2.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    //ignore
                }
                conn = null;
            }
        }
    }
}
