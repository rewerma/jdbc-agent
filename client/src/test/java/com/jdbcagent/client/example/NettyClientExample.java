package com.jdbcagent.client.example;

import com.jdbcagent.client.JdbcAgentDataSource;
import com.jdbcagent.client.jdbc.JdbcConnection;

import java.sql.*;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class NettyClientExample {
    public static void main(String[] args) throws SQLException {
        JdbcAgentDataSource jdbcAgentDataSource = null;

        try {
            jdbcAgentDataSource = new JdbcAgentDataSource();
            jdbcAgentDataSource.setUrl("jdbc:agent:127.0.0.1:10100/mytest");
            jdbcAgentDataSource.setUsername("test");
            jdbcAgentDataSource.setPassword("123456");


            final JdbcAgentDataSource jdbcAgentDataSource1 = jdbcAgentDataSource;
            ExecutorService executorService = Executors.newFixedThreadPool(5);
            for (int i = 0; i < 5; i++) {
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        test(jdbcAgentDataSource1);
//                        test01();
                    }
                });
            }

            executorService.shutdown();
            while (!executorService.isTerminated()) {
                Thread.sleep(100);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {

            if (jdbcAgentDataSource != null) {
                jdbcAgentDataSource.close();
            }
        }
    }


    public static void test(JdbcAgentDataSource jdbcAgentDataSource) {
        JdbcConnection conn = null;
        try {
            conn = (JdbcConnection) jdbcAgentDataSource.getConnection();

//            System.out.println(conn);


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
            stmt.close();


            PreparedStatement pstmt = conn.prepareStatement("update t_user set name=? where id=?");
            pstmt.setObject(1, "test_23");
            pstmt.setObject(2, 2L);
            int count = pstmt.executeUpdate();
            System.out.println("update count: " + count);
            pstmt.close();

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
                    System.out.println(conn.getRemoteId());
                } catch (Exception e) {
                    //ignore
                }
                conn = null;
            }
        }
    }

    public static void test01() {
        JdbcConnection conn = null;
        try {
            String URL = "jdbc:agent:127.0.0.1:10100/mytest";
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
}
