package com.jdbcagent.client.example;

import java.sql.*;

public class ZkClientExample {
    public static void main(String[] args) throws SQLException {
        for (int i = 0; i < 4; i++) {
            Connection conn = null;
            try {
                String URL = "jdbc:zookeeper:127.0.0.1:2181/mytest";
                String USER = "test";
                String PASSWORD = "123456";
                Class.forName("com.jdbcagent.client.jdbc.Driver");

                conn = DriverManager.getConnection(URL, USER, PASSWORD);

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

                PreparedStatement pstmt = conn.prepareStatement("select * from t_user where id!=?");
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
}
