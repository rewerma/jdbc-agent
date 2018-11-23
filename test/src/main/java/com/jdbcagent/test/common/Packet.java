package com.jdbcagent.test.common;

import java.io.Serializable;

public class Packet implements Serializable {
    private static final long serialVersionUID = -7639328310920562567L;

    private Integer key;
    private String classType;
    private String method;
    private Serializable[] params;

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

    public Serializable[] getParams() {
        return params;
    }

    public void setParams(Serializable[] params) {
        this.params = params;
    }
}
