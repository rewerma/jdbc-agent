package com.jdbcagent.test.netty;

import com.alibaba.druid.pool.DruidDataSource;
import com.esotericsoftware.reflectasm.MethodAccess;
import com.jdbcagent.test.common.JavaSerializeUtil;
import com.jdbcagent.test.common.Packet;
import com.jdbcagent.test.common.ResponsePacket;
import com.jdbcagent.test.server.ResultSetServer;

import java.io.Serializable;
import java.sql.*;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class TestInvoker {
    private static final ConcurrentHashMap<Integer, Connection> CONN_KEYS = new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<Integer, Statement> STMT_KEYS = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Integer, PreparedStatement> PRE_STMT_KEYS = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Integer, ResultSetServer> RS_KEYS = new ConcurrentHashMap<>();

    static DruidDataSource druidDataSource;

    static {
        druidDataSource = new DruidDataSource();
        druidDataSource.setDriverClassName("com.mysql.jdbc.Driver");
        druidDataSource.setUrl("jdbc:mysql://127.0.0.1:3306/mytest");
        druidDataSource.setUsername("root");
        druidDataSource.setPassword("121212");
        druidDataSource.setInitialSize(1);
        druidDataSource.setMinIdle(1);
        druidDataSource.setMaxActive(3);
        druidDataSource.setMaxWait(60000);
        druidDataSource.setTimeBetweenEvictionRunsMillis(60000);
        druidDataSource.setMinEvictableIdleTimeMillis(300000);
        druidDataSource.setMaxPoolPreparedStatementPerConnectionSize(20);
//        druidDataSource.setValidationQuery("select 1");
    }

    public static byte[] messageReceive(byte[] msg) {
        try {
            Packet packet = (Packet) JavaSerializeUtil.deserialize(msg);

            ResponsePacket responsePacket = new ResponsePacket();
            responsePacket.setKey(packet.getKey());
            responsePacket.setClassType(packet.getClassType());
            responsePacket.setMethod(packet.getMethod());

            String URL = "jdbc:mysql://127.0.0.1:3306/mytest";
            String USER = "root";
            String PASSWORD = "121212";
            Class.forName("com.mysql.jdbc.Driver");

            Properties info = new Properties();
            info.put("user", USER);
            info.put("password", PASSWORD);

//            Connection conn = DriverManager.getConnection(URL, info);

            if ("Connection".equalsIgnoreCase(packet.getClassType())) {
                if ("create".equalsIgnoreCase(packet.getMethod())) {
                    try {
//                        Connection conn = druidDataSource.getConnection();
                        Connection conn = DriverManager.getConnection(URL, info);

                        int key = conn.hashCode();
                        CONN_KEYS.put(key, conn);
                        responsePacket.setResult(key);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else {
                    Connection connection = CONN_KEYS.get(packet.getKey());
                    if (connection == null) {
                        //TODO
                        throw new RuntimeException("");
                    }
                    if (connection.isClosed()) {
                        //重新打开connection
                        connection = DriverManager.getConnection(URL, info); //druidDataSource.getConnection();
                        CONN_KEYS.put(packet.getKey(), connection);
                    }
                    if ("release".equalsIgnoreCase(packet.getMethod())) {
                        // 附加的release方法
                        connection.close();
                    } else {
                        MethodAccess access = MethodAccess.get(Connection.class);
                        Object res = access.invoke(connection, packet.getMethod(), (Object[]) packet.getParams());

                        if ("close".equalsIgnoreCase(packet.getMethod())) {
                            connection.close();
                            CONN_KEYS.remove(packet.getKey());
                        } else if ("createStatement".equalsIgnoreCase(packet.getMethod())) {
                            int key = res.hashCode();
                            STMT_KEYS.put(key, (Statement) res);
                            responsePacket.setResult(key);
                        } else if ("prepareStatement".equalsIgnoreCase(packet.getMethod())) {
                            int key = res.hashCode();
                            PRE_STMT_KEYS.put(key, (PreparedStatement) res);
                            responsePacket.setResult(key);
                        } else {
                            responsePacket.setResult((Serializable) res);
                        }
                    }
                }
            } else if ("Statement".equalsIgnoreCase(packet.getClassType())) {
                Statement stmt = STMT_KEYS.get(packet.getKey());
                if (stmt == null) {
                    //TODO
                    throw new RuntimeException("");
                }
                if(stmt.isClosed()){

                }
                MethodAccess access = MethodAccess.get(Statement.class);
                Object res = access.invoke(stmt, packet.getMethod(), (Object[]) packet.getParams());
                if ("close".equalsIgnoreCase(packet.getMethod())) {
                    stmt.close();
                    STMT_KEYS.remove(packet.getKey());
                } else if ("executeQuery".equalsIgnoreCase(packet.getMethod())) {
                    ResultSetServer resultSetServer = new ResultSetServer((ResultSet) res);
                    int key = res.hashCode();
                    RS_KEYS.put(key, resultSetServer);
                    responsePacket.setResult(key);
                } else {
                    responsePacket.setResult((Serializable) res);
                }
            } else if ("PreparedStatement".equalsIgnoreCase(packet.getClassType())) {
                PreparedStatement pstmt = PRE_STMT_KEYS.get(packet.getKey());
                if (pstmt == null) {
                    //TODO
                    throw new RuntimeException("");
                }
                MethodAccess access = MethodAccess.get(PreparedStatement.class);
                Object res = access.invoke(pstmt, packet.getMethod(), (Object[]) packet.getParams());
                if ("close".equalsIgnoreCase(packet.getMethod())) {
                    pstmt.close();
                    PRE_STMT_KEYS.remove(packet.getKey());
                } else if ("executeQuery".equalsIgnoreCase(packet.getMethod())) {
                    ResultSetServer resultSetServer = new ResultSetServer((ResultSet) res);
                    int key = res.hashCode();
                    RS_KEYS.put(key, resultSetServer);
                    responsePacket.setResult(key);
                } else {
                    responsePacket.setResult((Serializable) res);
                }
            } else if ("ResultSet".equalsIgnoreCase(packet.getClassType())) {
                ResultSetServer rs = RS_KEYS.get(packet.getKey());
                if (rs == null) {
                    //TODO
                    throw new RuntimeException("");
                }
                MethodAccess access = MethodAccess.get(ResultSetServer.class);
                Object res = access.invoke(rs, packet.getMethod(), (Object[]) packet.getParams());
                if ("close".equalsIgnoreCase(packet.getMethod())) {
                    rs.close();
                    RS_KEYS.remove(packet.getKey());
                } else {
                    responsePacket.setResult((Serializable) res);
                }
            }

            return JavaSerializeUtil.serialize(responsePacket);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
