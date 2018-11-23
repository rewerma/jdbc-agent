package com.jdbcagent.test.common.support.serial;

import java.io.Serializable;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.util.Properties;

/**
 * JDBC-Agent serial connection
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class SerialConnection implements Serializable {
    private static final long serialVersionUID = -5199367115781562435L;

    private boolean autoCommit;
    private boolean readOnly;
    private String catalog;
    private int transactionIsolatio;
    private String warnings;
    private int holdability;
    private Properties clientInfo;
    private String schema;

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        this.autoCommit = autoCommit;
    }

    public boolean getAutoCommit() throws SQLException {
        return this.autoCommit;
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        this.readOnly = readOnly;
    }

    public boolean isReadOnly() throws SQLException {
        return this.readOnly;
    }

    public void setCatalog(String catalog) throws SQLException {
        this.catalog = catalog;
    }

    public String getCatalog() throws SQLException {
        return this.catalog;
    }

    public void setTransactionIsolation(int level) throws SQLException {
        this.transactionIsolatio = level;
    }

    public int getTransactionIsolation() throws SQLException {
        return this.transactionIsolatio;
    }

    public void setHoldability(int holdability) throws SQLException {
        this.holdability = holdability;
    }

    public int getHoldability() throws SQLException {
        return this.holdability;
    }

    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        if (clientInfo == null) {
            clientInfo = new Properties();
        }
        clientInfo.put(name, value);
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        this.clientInfo = properties;
    }

    public String getClientInfo(String name) throws SQLException {
        return (String) clientInfo.get(name);
    }

    public Properties getClientInfo() throws SQLException {
        return this.clientInfo;
    }

    public void setSchema(String schema) throws SQLException {
        this.schema = schema;
    }

    public String getSchema() throws SQLException {
        return this.schema;
    }
}
