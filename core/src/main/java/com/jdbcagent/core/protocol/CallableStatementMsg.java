package com.jdbcagent.core.protocol;

import java.io.Serializable;

import com.jdbcagent.core.protocol.PreparedStatementMsg.ParamType;

/**
 * JDBC-Agent protocol CallableStatementMsg
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class CallableStatementMsg extends Message implements Serializable {
    private static final long serialVersionUID = 3685743926696474526L;
    private Long id;
    private Integer parameterIndex;
    private String parameterName;
    private Method method;
    private Serializable[] params;
    private ParamType paramType;
    private Serializable response;

    public static Builder newBuilder() {
        return new Builder(new CallableStatementMsg());
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Integer getParameterIndex() {
        return parameterIndex;
    }

    public void setParameterIndex(Integer parameterIndex) {
        this.parameterIndex = parameterIndex;
    }

    public String getParameterName() {
        return parameterName;
    }

    public void setParameterName(String parameterName) {
        this.parameterName = parameterName;
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

    public ParamType getParamType() {
        return paramType;
    }

    public void setParamType(ParamType paramType) {
        this.paramType = paramType;
    }

    public Serializable getResponse() {
        return response;
    }

    public void setResponse(Serializable response) {
        this.response = response;
    }

    public static class Builder {
        private CallableStatementMsg callableStatementMsg;

        public Builder(CallableStatementMsg callableStatementMsg) {
            this.callableStatementMsg = callableStatementMsg;
        }

        public Builder setId(Long id) {
            callableStatementMsg.setId(id);
            return this;
        }

        public Builder setParameterIndex(int parameterIndex) {
            callableStatementMsg.setParameterIndex(parameterIndex);
            return this;
        }

        public Builder setParameterName(String parameterName) {
            callableStatementMsg.setParameterName(parameterName);
            return this;
        }

        public Builder setMethod(Method method) {
            callableStatementMsg.setMethod(method);
            return this;
        }

        public Builder setParamType(ParamType paramType) {
            callableStatementMsg.setParamType(paramType);
            return this;
        }

        public Builder setParams(Serializable[] params) {
            callableStatementMsg.setParams(params);
            return this;
        }

        public Builder setResponse(Serializable response) {
            callableStatementMsg.setResponse(response);
            return this;
        }

        public CallableStatementMsg build() {
            return callableStatementMsg;
        }
    }

    public enum Method {
        executeQuery,
        executeUpdate,
        clearParameters,
        execute,

        wasNull,
        getString,
        getBoolean,
        getByte,
        getShort,
        getInt,
        getLong,
        getFloat,
        getDouble,
        getBigDecimal,
        getBytes,
        getDate,
        getTime,
        getTimestamp,
        getObject,
        getRef,
        getBlob,
        getClob,
        getArray,
        getURL,
        getNClob,
        getNString,
        getCharacterStream,
        getNCharacterStream,
        getRowId
    }
}
