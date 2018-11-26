package com.jdbcagent.test;


import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.druid.pool.DruidPooledPreparedStatement;
import com.jdbcagent.test.pool.JADruidDataSource;

import java.sql.*;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Test {
    public static void main(String[] args) throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(5);

        final JADruidDataSource druidDataSource = new JADruidDataSource();
        druidDataSource.setDriverClassName("com.jdbcagent.test.jdbc.Driver");
        druidDataSource.setUrl("jdbc:agent:127.0.0.1:10101/mytest");
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

        for (int i = 0; i < 5; i++) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        Connection conn = druidDataSource.getConnection();

//                        String URL = "jdbc:agent:127.0.0.1:10101/mytest";
//                        String USER = "test";
//                        String PASSWORD = "123456";
//                        Class.forName("com.jdbcagent.test.jdbc.Driver");
//
//                        Properties info = new Properties();
//                        info.put("user", USER);
//                        info.put("password", PASSWORD);
//                        info.put("timeout", "600000");
//
//                        Connection conn = DriverManager.getConnection(URL, info);

                        String testSql = conn.nativeSQL("select * from t_user");
                        System.out.println(testSql);
//
                        Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery(testSql);
                        while (rs.next()) {
                            System.out.println(rs.getLong("id") + " " + rs.getString("name") + " "
                                    + rs.getInt("gender") + " " + rs.getString("email") + " "
                                    + rs.getTimestamp("sys_time"));
                        }
                        rs.close();
                        stmt.close();

                        PreparedStatement pstmt = conn.prepareStatement("select * from t_user");
                         rs = pstmt.executeQuery();
                        while (rs.next()) {
                            System.out.println(rs.getLong("id") + " " + rs.getString("name") + " "
                                    + rs.getInt("gender") + " " + rs.getString("email") + " "
                                    + rs.getTimestamp("sys_time"));
                        }
                        rs.close();
                        pstmt.close();
//
//                        ((DruidPooledConnection) conn).closePoolableStatement((DruidPooledPreparedStatement) pstmt);

                        conn.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        executorService.shutdown();
        while (!executorService.isTerminated()) {
            Thread.sleep(100);
        }

//        Thread.sleep(10000);


        druidDataSource.close();
    }
}
