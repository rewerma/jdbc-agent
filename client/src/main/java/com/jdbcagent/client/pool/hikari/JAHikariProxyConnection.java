package com.jdbcagent.client.pool.hikari;

import com.jdbcagent.client.jdbc.JdbcConnection;
import com.zaxxer.hikari.pool.HikariProxyConnection;
import com.zaxxer.hikari.pool.ProxyConnection;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class JAHikariProxyConnection implements Connection {
    private HikariProxyConnection conn;

    public JAHikariProxyConnection(HikariProxyConnection conn) {
        this.conn = conn;
    }

    @Override
    public Statement createStatement() throws SQLException {
        return conn.createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String var1) throws SQLException {
        return conn.prepareStatement(var1);
    }

    @Override
    public CallableStatement prepareCall(String var1) throws SQLException {
        return conn.prepareCall(var1);
    }

    @Override
    public String nativeSQL(String var1) throws SQLException {
        return conn.nativeSQL(var1);
    }

    @Override
    public void setAutoCommit(boolean var1) throws SQLException {
        conn.setAutoCommit(var1);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return conn.getAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
        conn.commit();
    }

    @Override
    public void rollback() throws SQLException {
        conn.rollback();
    }

    @Override
    public void close() throws SQLException {
        JdbcConnection jdbcConn = null;
            try {
                Field innerField = ProxyConnection.class.getDeclaredField("delegate");
                innerField.setAccessible(true);
                jdbcConn = (JdbcConnection) innerField.get(conn);
            } catch (Exception e) {
                e.printStackTrace();
            }
        conn.close();
        if (jdbcConn != null) {
            jdbcConn.release();
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return conn.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return conn.getMetaData();
    }

    @Override
    public void setReadOnly(boolean var1) throws SQLException {
        conn.setReadOnly(var1);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return conn.isReadOnly();
    }

    @Override
    public void setCatalog(String var1) throws SQLException {
        conn.setCatalog(var1);
    }

    @Override
    public String getCatalog() throws SQLException {
        return conn.getCatalog();
    }

    @Override
    public void setTransactionIsolation(int var1) throws SQLException {
        conn.setTransactionIsolation(var1);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return conn.getTransactionIsolation();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return conn.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        conn.clearWarnings();
    }

    @Override
    public Statement createStatement(int var1, int var2) throws SQLException {
        return conn.createStatement(var1, var2);
    }

    @Override
    public PreparedStatement prepareStatement(String var1, int var2, int var3) throws SQLException {
        return conn.prepareStatement(var1, var2, var3);
    }

    @Override
    public CallableStatement prepareCall(String var1, int var2, int var3) throws SQLException {
        return conn.prepareCall(var1, var2, var3);
    }

    @Override
    public Map getTypeMap() throws SQLException {
        return conn.getTypeMap();
    }

    @Override
    public void setTypeMap(Map var1) throws SQLException {
        conn.setTypeMap(var1);
    }

    @Override
    public void setHoldability(int var1) throws SQLException {
        conn.setHoldability(var1);
    }

    @Override
    public int getHoldability() throws SQLException {
        return conn.getHoldability();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return conn.setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String var1) throws SQLException {
        return conn.setSavepoint(var1);
    }

    @Override
    public void rollback(Savepoint var1) throws SQLException {
        conn.rollback(var1);
    }

    @Override
    public void releaseSavepoint(Savepoint var1) throws SQLException {
        conn.releaseSavepoint(var1);
    }

    @Override
    public Statement createStatement(int var1, int var2, int var3) throws SQLException {
        return conn.createStatement(var1, var2, var3);
    }

    @Override
    public PreparedStatement prepareStatement(String var1, int var2, int var3, int var4) throws SQLException {
        return conn.prepareStatement(var1, var2, var3);
    }

    @Override
    public CallableStatement prepareCall(String var1, int var2, int var3, int var4) throws SQLException {
        return conn.prepareCall(var1, var2, var3, var4);
    }

    @Override
    public PreparedStatement prepareStatement(String var1, int var2) throws SQLException {
        return conn.prepareStatement(var1, var2);
    }

    @Override
    public PreparedStatement prepareStatement(String var1, int[] var2) throws SQLException {
        return conn.prepareStatement(var1, var2);
    }

    @Override
    public PreparedStatement prepareStatement(String var1, String[] var2) throws SQLException {
        return conn.prepareStatement(var1, var2);
    }

    @Override
    public Clob createClob() throws SQLException {
        return conn.createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        return conn.createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return conn.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return conn.createSQLXML();
    }

    @Override
    public boolean isValid(int var1) throws SQLException {
        return conn.isValid(var1);
    }

    @Override
    public void setClientInfo(String var1, String var2) throws SQLClientInfoException {
        conn.setClientInfo(var1, var2);
    }

    @Override
    public void setClientInfo(Properties var1) throws SQLClientInfoException {
        conn.setClientInfo(var1);
    }

    @Override
    public String getClientInfo(String var1) throws SQLException {
        return conn.getClientInfo(var1);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return conn.getClientInfo();
    }

    @Override
    public Array createArrayOf(String var1, Object[] var2) throws SQLException {
        return conn.createArrayOf(var1, var2);
    }

    @Override
    public Struct createStruct(String var1, Object[] var2) throws SQLException {
        return conn.createStruct(var1, var2);
    }

    @Override
    public void setSchema(String var1) throws SQLException {
        conn.setSchema(var1);
    }

    @Override
    public String getSchema() throws SQLException {
        return conn.getSchema();
    }

    @Override
    public void abort(Executor var1) throws SQLException {
        conn.abort(var1);
    }

    @Override
    public void setNetworkTimeout(Executor var1, int var2) throws SQLException {
        conn.setNetworkTimeout(var1, var2);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return conn.getNetworkTimeout();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return conn.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return conn.isWrapperFor(iface);
    }
}
