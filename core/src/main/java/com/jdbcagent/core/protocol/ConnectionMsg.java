package com.jdbcagent.core.protocol;

import com.jdbcagent.core.support.serial.SerialConnection;

import java.io.Serializable;

/**
 * JDBC-Agent protocol ConnectionMsg
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class ConnectionMsg extends Message implements Serializable {
    private static final long serialVersionUID = -4492887915069072803L;

    private Long id;
    private String catalog;
    private String username;
    private String password;
    private SerialConnection serialConnection;
    private Serializable[] params;
    private Serializable response;
    private Method method;
    private String warnings;

    public static Builder newBuilder() {
        return new Builder(new ConnectionMsg());
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Serializable[] getParams() {
        return params;
    }

    public void setParams(Serializable[] params) {
        this.params = params;
    }

    public Serializable getResponse() {
        return response;
    }

    public void setResponse(Serializable response) {
        this.response = response;
    }

    public Method getMethod() {
        return method;
    }

    public void setMethod(Method method) {
        this.method = method;
    }

    public SerialConnection getSerialConnection() {
        return serialConnection;
    }

    public void setSerialConnection(SerialConnection serialConnection) {
        this.serialConnection = serialConnection;
    }

    public String getWarnings() {
        return warnings;
    }

    public void setWarnings(String warnings) {
        this.warnings = warnings;
    }

    public static class Builder {
        private ConnectionMsg connectMsg;

        public Builder(ConnectionMsg connectMsg) {
            this.connectMsg = connectMsg;
        }

        public Builder setId(Long id) {
            connectMsg.setId(id);
            return this;
        }

        public Builder setCatalog(String catalog) {
            connectMsg.setCatalog(catalog);
            return this;
        }

        public Builder setUsername(String username) {
            connectMsg.setUsername(username);
            return this;
        }

        public Builder setPassword(String password) {
            connectMsg.setPassword(password);
            return this;
        }

        public Builder setParams(Serializable[] params) {
            connectMsg.setParams(params);
            return this;
        }

        public Builder setResponse(Serializable response) {
            connectMsg.setResponse(response);
            return this;
        }

        public Builder setMethod(Method method) {
            connectMsg.setMethod(method);
            return this;
        }

        public Builder setSerialConnection(SerialConnection serialConnection) {
            connectMsg.setSerialConnection(serialConnection);
            return this;
        }

        public Builder setWarnings(String warnings) {
            connectMsg.setWarnings(warnings);
            return this;
        }

        public ConnectionMsg build() {
            return connectMsg;
        }
    }

    public enum Method {
        createStatement,
        prepareStatement,
        prepareCall,
        getMetaData,
        nativeSQL,
        setAutoCommit,
        getAutoCommit,
        commit,
        rollback,
        isClosed,
        setReadOnly,
        isReadOnly,
        setCatalog,
        getCatalog,
        setTransactionIsolation,
        getTransactionIsolation,
        getWarnings,
        clearWarnings,
        getTypeMap,
        setTypeMap,
        setHoldability,
        getHoldability,
        setSavepoint,
        releaseSavepoint,
        createClob,
        createBlob,
        createNClob,
        isValid,
        setClientInfo,
        getClientInfo,
        createArrayOf,
        createStruct,
        setSchema,
        getSchema,
        getNetworkTimeout,
        release
    }
}
