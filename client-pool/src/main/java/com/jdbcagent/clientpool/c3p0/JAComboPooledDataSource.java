package com.jdbcagent.clientpool.c3p0;

import com.mchange.v2.c3p0.AbstractComboPooledDataSource;

import javax.naming.Referenceable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

public class JAComboPooledDataSource extends AbstractComboPooledDataSource implements Serializable, Referenceable {
    private static final long serialVersionUID = -6642949659706208880L;

    public JAComboPooledDataSource() {
        super();
    }

    public JAComboPooledDataSource(boolean autoregister) {
        super(autoregister);
    }

    public JAComboPooledDataSource(String configName) {
        super(configName);
    }


    // serialization stuff -- set up bound/constrained property event handlers on deserialization
    private static final short VERSION = 0x0009;

    private void writeObject(ObjectOutputStream oos) throws IOException {
        oos.writeShort(VERSION);
    }

    private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
        short version = ois.readShort();
        switch (version) {
            case VERSION:
                //ok
                break;
            default:
                throw new IOException("Unsupported Serialized Version: " + version);
        }
    }


    @Override
    public Connection getConnection() throws SQLException {
        return new JAC3p0Connection(super.getConnection());
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return new JAC3p0Connection(super.getConnection());
    }
}
