package com.jdbcagent.test.common;

import java.io.Serializable;

public class ResponsePacket implements Serializable {
    private static final long serialVersionUID = 994305731688823158L;

    private Integer key;
    private String classType;
    private String method;
    private Serializable result;

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public Integer getKey() {
        return key;
    }

    public void setKey(Integer key) {
        this.key = key;
    }

    public String getClassType() {
        return classType;
    }

    public void setClassType(String classType) {
        this.classType = classType;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public Serializable getResult() {
        return result;
    }

    public void setResult(Serializable result) {
        this.result = result;
    }
}
