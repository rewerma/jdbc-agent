package com.jdbcagent.core.support.serial;

import java.io.Serializable;
import java.sql.RowId;

/**
 * JDBC-Agent serial rowId
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class SerialRowId implements RowId, Serializable {
    private static final long serialVersionUID = 5605376909854047847L;

    private byte[] bytes;

    public SerialRowId(byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public byte[] getBytes() {
        return bytes;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }
}
