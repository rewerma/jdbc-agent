package com.jdbcagent.core.protocol;

import java.io.Serializable;

public class ClientAuth extends Message implements Serializable {
    private static final long serialVersionUID = 8294482765992626091L;

    private String username;
    private String password;
    private int netReadTimeout = 5 * 60 * 1000;
    private int netWriteTimeout = 5 * 60 * 1000;

    public static Builder newBuilder() {
        return new Builder(new ClientAuth());
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

    public int getNetReadTimeout() {
        return netReadTimeout;
    }

    public void setNetReadTimeout(int netReadTimeout) {
        this.netReadTimeout = netReadTimeout;
    }

    public int getNetWriteTimeout() {
        return netWriteTimeout;
    }

    public void setNetWriteTimeout(int netWriteTimeout) {
        this.netWriteTimeout = netWriteTimeout;
    }

    public static class Builder {
        private ClientAuth clientAuth;

        public Builder(ClientAuth clientAuth) {
            this.clientAuth = clientAuth;
        }

        public Builder setUsername(String username) {
            clientAuth.setUsername(username);
            return this;
        }

        public Builder setPassword(String password) {
            clientAuth.setPassword(password);
            return this;
        }

        public Builder setNetReadTimeout(int netReadTimeout) {
            clientAuth.setNetReadTimeout(netReadTimeout);
            return this;
        }

        public Builder setNetWriteTimeout(int netWriteTimeout) {
            clientAuth.setNetWriteTimeout(netWriteTimeout);
            return this;
        }

        public ClientAuth build() {
            return clientAuth;
        }
    }
}
