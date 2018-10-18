package com.jdbcagent.client.example;

import com.jdbcagent.client.JdbcAgentDataSource;

import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class NettyClientExample {
    private static volatile boolean run = false;

    public static void main(String[] args) throws SQLException {
        JdbcAgentDataSource jdbcAgentDataSource = null;
//        Connection conn = null;
        try {
            jdbcAgentDataSource = new JdbcAgentDataSource();
            jdbcAgentDataSource.setUrl("jdbc:agent:127.0.0.1:10101/mytest");
            jdbcAgentDataSource.setUsername("test");
            jdbcAgentDataSource.setPassword("123456");
//            conn = jdbcAgentDataSource.getConnection();
//
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
//
//            PreparedStatement pstmt = conn.prepareStatement("update t_user set name=? where id=?");
//            pstmt.setObject(1, "test_23");
//            pstmt.setObject(2, 2L);
//            int count = pstmt.executeUpdate();
//            System.out.println("update count: " + count);
//            pstmt.close();
//
//            PreparedStatement pstmt2 = conn.prepareStatement("select * from t_user where id=?");
//            pstmt2.setLong(1, 2L);
//            rs = pstmt2.executeQuery();
//            while (rs.next()) {
//                System.out.println(rs.getLong("id") + " " + rs.getString("name") + " "
//                        + rs.getInt("gender") + " " + rs.getString("email") + " "
//                        + rs.getTimestamp("sys_time"));
//            }
//            rs.close();
//            pstmt2.close();
            final JdbcAgentDataSource jdbcAgentDataSource1 = jdbcAgentDataSource;
            ExecutorService executorService = Executors.newFixedThreadPool(5);
            final AtomicInteger a = new AtomicInteger(0);
            for (int i = 0; i < 3; i++) {
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
//                        System.out.println("------------------- " + a.incrementAndGet());
                        test(jdbcAgentDataSource1);
                    }
                });
            }
            run = true;

            Thread.sleep(5000);
            executorService.shutdown();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
//            if (conn != null) {
//                conn.close();
//                conn = null;
//            }
            if (jdbcAgentDataSource != null) {
                jdbcAgentDataSource.close();
            }
        }
    }

    private static void test(JdbcAgentDataSource jdbcAgentDataSource) {
        while (!run) ;

        Connection conn = null;
        try {
            conn = jdbcAgentDataSource.getConnection();

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
                } catch (SQLException e) {
                    //ignore
                }
                conn = null;
            }
        }
    }
}
