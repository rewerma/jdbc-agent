package com.jdbcagent.core.support.serial;

import javax.sql.rowset.serial.SerialClob;
import javax.sql.rowset.serial.SerialException;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;

/**
 * JDBC-Agent serial NClob
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class SerialNClob extends SerialClob implements NClob {
    private static final long serialVersionUID = 589585256528246446L;

    public SerialNClob(char[] ch) throws SerialException, SQLException {
        super(ch);
    }

    public SerialNClob(Clob clob) throws SerialException, SQLException {
        super(clob);
    }
}
