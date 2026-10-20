package com.jdbcagent.core.protocol;

import java.io.Serializable;

/**
 * 大字段分块读取和关闭。
 */
public class LobMsg extends Message implements Serializable {
    private static final long serialVersionUID = 1L;

    private Long lobId;
    private Long position;
    private Integer length;
    private byte[] data;
    private boolean eof;

    public LobMsg() {
    }

    public static Builder newBuilder() {
        return new Builder(new LobMsg());
    }

    public Long getLobId() {
        return lobId;
    }

    public void setLobId(Long lobId) {
        this.lobId = lobId;
    }

    public Long getPosition() {
        return position;
    }

    public void setPosition(Long position) {
        this.position = position;
    }

    public Integer getLength() {
        return length;
    }

    public void setLength(Integer length) {
        this.length = length;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public boolean isEof() {
        return eof;
    }

    public void setEof(boolean eof) {
        this.eof = eof;
    }

    public static class Builder {
        private final LobMsg lobMsg;

        public Builder(LobMsg lobMsg) {
            this.lobMsg = lobMsg;
        }

        public Builder setLobId(Long lobId) {
            lobMsg.setLobId(lobId);
            return this;
        }

        public Builder setPosition(Long position) {
            lobMsg.setPosition(position);
            return this;
        }

        public Builder setLength(Integer length) {
            lobMsg.setLength(length);
            return this;
        }

        public Builder setData(byte[] data) {
            lobMsg.setData(data);
            return this;
        }

        public Builder setEof(boolean eof) {
            lobMsg.setEof(eof);
            return this;
        }

        public LobMsg build() {
            return lobMsg;
        }
    }
}
