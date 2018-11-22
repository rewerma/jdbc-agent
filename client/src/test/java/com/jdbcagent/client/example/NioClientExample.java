package com.jdbcagent.client.example;

import java.sql.*;
import java.util.Properties;

public class NioClientExample {
    public static void main(String[] args) throws SQLException {
        Connection conn = null;
        try {
            String URL = "jdbc:agent:127.0.0.1:10100/mytest";
            String USER = "test";
            String PASSWORD = "123456";
            Class.forName("com.jdbcagent.client.jdbc.Driver");

            Properties info = new Properties();
            info.put("user", USER);
            info.put("password", PASSWORD);
            info.put("timeout", "600000");

            conn = DriverManager.getConnection(URL, info);

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

            PreparedStatement pstmt = conn.prepareStatement("select * from t_user where id=?");
            pstmt.setLong(1, 2L);
            ResultSet rs = pstmt.executeQuery();
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
                conn = null;
            }
        }
    }
}
