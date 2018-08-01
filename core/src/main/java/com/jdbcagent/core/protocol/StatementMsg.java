package com.jdbcagent.core.protocol;

import java.io.Serializable;

/**
 * JDBC-Agent protocol StatementMsg
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class StatementMsg extends Message implements Serializable {
    private static final long serialVersionUID = 3766702983542100378L;

    private Long id;
    private String sql;
    private Method method;
    private Serializable[] params;
    private Serializable response;

    public static Builder newBuilder() {
        return new Builder(new StatementMsg());
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public Method getMethod() {
        return method;
    }

    public void setMethod(Method method) {
        this.method = method;
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

    public static class Builder {
        private StatementMsg statementMsg;

        public Builder(StatementMsg statementMsg) {
            this.statementMsg = statementMsg;
        }

        public Builder setId(Long id) {
            statementMsg.setId(id);
            return this;
        }

        public Builder setSql(String sql) {
            statementMsg.setSql(sql);
            return this;
        }

        public Builder setMethod(Method method) {
            statementMsg.setMethod(method);
            return this;
        }

        public Builder setParams(Serializable[] params) {
            statementMsg.setParams(params);
            return this;
        }

        public Builder setResponse(Serializable response) {
            statementMsg.setResponse(response);
            return this;
        }

        public StatementMsg build() {
            return statementMsg;
        }
    }

    public enum Method {
        executeQuery,
        getMaxFieldSize,
        setMaxFieldSize,
        getMaxRows,
        setMaxRows,
        setEscapeProcessing,
        getQueryTimeout,
        setQueryTimeout,
        cancel,
        getWarnings,
        clearWarnings,
        setCursorName,
        execute,
        getUpdateCount,
        getMoreResults,
        setFetchDirection,
        getFetchDirection,
        setFetchSize,
        getFetchSize,
        getResultSetConcurrency,
        getResultSetType,
        addBatch,
        clearBatch,
        executeBatch,
        executeUpdate,
        getResultSetHoldability,
        isClosed,
        setPoolable,
        isPoolable,
        closeOnCompletion,
        isCloseOnCompletion,
        close
    }
}
