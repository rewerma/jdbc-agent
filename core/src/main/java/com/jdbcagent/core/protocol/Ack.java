package com.jdbcagent.core.protocol;

import java.io.Serializable;

/**
 * JDBC-Agent protocol Ack
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class Ack extends Message implements Serializable {
    private static final long serialVersionUID = 5014287685987791275L;

    private int errorCode = 0;
    private String errorMessage;

    public static Builder newBuilder() {
        return new Builder(new Ack());
    }

    public int getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public static class Builder {
        private Ack ack;

        public Builder(Ack ack) {
            this.ack = ack;
        }

        public Builder setErrorCode(int errorCode) {
            ack.setErrorCode(errorCode);
            return this;
        }

        public Builder setErrorMessage(String errorMessage) {
            ack.setErrorMessage(errorMessage);
            return this;
        }

        public Ack build() {
            return ack;
        }
    }
}
