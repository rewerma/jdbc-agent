package com.jdbcagent.core.protocol;

import java.io.Serializable;

/**
 * JDBC-Agent protocol PreparedStatementMsg
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class PreparedStatementMsg extends Message implements Serializable {
    private static final long serialVersionUID = -7421369353006342869L;

    private Long id;
    private int parameterIndex;
    private Method method;
    private Serializable[] params;
    private ParamType paramType;
    private Serializable response;

    public static Builder newBuilder() {
        return new Builder(new PreparedStatementMsg());
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public int getParameterIndex() {
        return parameterIndex;
    }

    public void setParameterIndex(int parameterIndex) {
        this.parameterIndex = parameterIndex;
    }

    public Method getMethod() {
        return method;
    }

    public void setMethod(Method method) {
        this.method = method;
    }

    public ParamType getParamType() {
        return paramType;
    }

    public void setParamType(ParamType paramType) {
        this.paramType = paramType;
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
        private PreparedStatementMsg preparedStatementMsg;

        public Builder(PreparedStatementMsg preparedStatementMsg) {
            this.preparedStatementMsg = preparedStatementMsg;
        }

        public Builder setId(Long id) {
            preparedStatementMsg.setId(id);
            return this;
        }

        public Builder setParameterIndex(int parameterIndex) {
            preparedStatementMsg.setParameterIndex(parameterIndex);
            return this;
        }

        public Builder setMethod(Method method) {
            preparedStatementMsg.setMethod(method);
            return this;
        }

        public Builder setParamType(ParamType paramType) {
            preparedStatementMsg.setParamType(paramType);
            return this;
        }

        public Builder setParams(Serializable[] params) {
            preparedStatementMsg.setParams(params);
            return this;
        }

        public Builder setResponse(Serializable response) {
            preparedStatementMsg.setResponse(response);
            return this;
        }

        public PreparedStatementMsg build() {
            return preparedStatementMsg;
        }
    }

    public enum Method {
        executeQuery,
        executeUpdate,
        clearParameters,
        execute,
        addBatch
    }

    public enum ParamType {
        NULL,
        BOOLEAN,
        BYTE,
        SHORT,
        INT,
        LONG,
        FLOAT,
        DOUBLE,
        BIG_DECIMAL,
        STRING,
        BYTES,
        DATE,
        TIME,
        TIMESTAMP,
        URL,
        OBJECT,
        REF,
        BLOB,
        CLOB,
        NCLOB,
        NSTRING,
        ARRAY,
        ROW_ID,
        NCHARACTER_STREAM,
        CHARACTER_STREAM,
        BINARY_STREAM,
        ASCII_STREAM,
        UNICODE_STREAM,

        registerOutParameter
    }
}
