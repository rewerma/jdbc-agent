package com.jdbcagent.test.common.support.serial;

import java.io.Serializable;
import java.sql.SQLException;
import java.sql.Savepoint;

/**
 * JDBC-Agent serial savepoint
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class SerialSavepoint implements Savepoint, Serializable {
    private static final long serialVersionUID = -1032359760920147959L;

    private int savepointId;
    private String savepointName;

    public SerialSavepoint(int savepointId, String savepointName) {
        this.savepointId = savepointId;
        this.savepointName = savepointName;
    }

    @Override
    public int getSavepointId() throws SQLException {
        return savepointId;
    }

    @Override
    public String getSavepointName() throws SQLException {
        return savepointName;
    }
}
