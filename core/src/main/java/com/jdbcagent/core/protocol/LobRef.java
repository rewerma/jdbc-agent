package com.jdbcagent.core.protocol;

import java.io.Serializable;

/**
 * 大字段在行数据里的句柄。内容在服务端临时文件中，客户端按块拉取。
 */
public class LobRef implements Serializable {
    private static final long serialVersionUID = 1L;

    private long lobId;
    private int sqlType;
    /** 二进制为字节数，字符类型为字符数。 */
    private long length;
    private int chunkSize;

    public LobRef() {
    }

    public LobRef(long lobId, int sqlType, long length, int chunkSize) {
        this.lobId = lobId;
        this.sqlType = sqlType;
        this.length = length;
        this.chunkSize = chunkSize;
    }

    public long getLobId() {
        return lobId;
    }

    public void setLobId(long lobId) {
        this.lobId = lobId;
    }

    public int getSqlType() {
        return sqlType;
    }

    public void setSqlType(int sqlType) {
        this.sqlType = sqlType;
    }

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    @Override
    public String toString() {
        return "LobRef[id=" + lobId + ",type=" + sqlType + ",length=" + length + "]";
    }
}
