package com.jdbcagent.core.util;

import com.esotericsoftware.kryo.Kryo;
import com.jdbcagent.core.protocol.Ack;
import com.jdbcagent.core.protocol.CallableStatementMsg;
import com.jdbcagent.core.protocol.ClientAuth;
import com.jdbcagent.core.protocol.ConnectionMsg;
import com.jdbcagent.core.protocol.DatabaseMetaDataMsg;
import com.jdbcagent.core.protocol.LobMsg;
import com.jdbcagent.core.protocol.LobRef;
import com.jdbcagent.core.protocol.Packet;
import com.jdbcagent.core.protocol.PreparedStatementMsg;
import com.jdbcagent.core.protocol.ResultSetMsg;
import com.jdbcagent.core.protocol.StatementMsg;
import com.jdbcagent.core.support.JdbcRowSetResourceBundle;
import com.jdbcagent.core.support.SerialRowSetImpl;
import com.jdbcagent.core.support.SerialRowSetMetaDataImpl;
import com.jdbcagent.core.support.internal.InsertRow;
import com.jdbcagent.core.support.internal.Row;
import com.jdbcagent.core.support.internal.SerialRowSetReader;
import com.jdbcagent.core.support.internal.SerialRowSetWriter;
import com.jdbcagent.core.support.serial.SerialConnection;
import com.jdbcagent.core.support.serial.SerialNClob;
import com.jdbcagent.core.support.serial.SerialRowId;
import com.jdbcagent.core.support.serial.SerialSavepoint;
import com.jdbcagent.core.support.serial.SerialVoid;

import javax.sql.rowset.serial.SerialArray;
import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import javax.sql.rowset.serial.SerialRef;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Properties;
import java.util.UUID;
import java.util.Vector;

/**
 * 协议允许出现在报文里的类型。客户端和服务端必须按同一顺序登记。
 */
final class KryoClassRegistry {
    private static final java.util.Set<String> NAMES = new java.util.HashSet<String>();

    private KryoClassRegistry() {
    }

    static void register(Kryo kryo) {
        register(kryo, String.class);
        register(kryo, Packet.class);
        register(kryo, Packet.PacketType.class);
        register(kryo, Ack.class);
        register(kryo, ClientAuth.class);
        register(kryo, ConnectionMsg.class);
        register(kryo, ConnectionMsg.Method.class);
        register(kryo, StatementMsg.class);
        register(kryo, StatementMsg.Method.class);
        register(kryo, PreparedStatementMsg.class);
        register(kryo, PreparedStatementMsg.Method.class);
        register(kryo, PreparedStatementMsg.ParamType.class);
        register(kryo, CallableStatementMsg.class);
        register(kryo, CallableStatementMsg.Method.class);
        register(kryo, DatabaseMetaDataMsg.class);
        register(kryo, DatabaseMetaDataMsg.Method.class);
        register(kryo, ResultSetMsg.class);

        register(kryo, SerialConnection.class);
        register(kryo, SerialNClob.class);
        register(kryo, SerialRowId.class);
        register(kryo, SerialSavepoint.class);
        register(kryo, SerialVoid.class);
        register(kryo, SerialRowSetImpl.class);
        register(kryo, SerialRowSetMetaDataImpl.class);
        register(kryo, colInfoClass());
        register(kryo, colInfoArrayClass());
        register(kryo, JdbcRowSetResourceBundle.class);
        register(kryo, Row.class);
        register(kryo, InsertRow.class);
        register(kryo, SerialRowSetReader.class);
        register(kryo, SerialRowSetWriter.class);

        register(kryo, ArrayList.class);
        register(kryo, LinkedList.class);
        register(kryo, HashMap.class);
        register(kryo, LinkedHashMap.class);
        register(kryo, Hashtable.class);
        register(kryo, HashSet.class);
        register(kryo, Vector.class);
        register(kryo, Properties.class);
        register(kryo, BitSet.class);
        register(kryo, Calendar.class);
        register(kryo, GregorianCalendar.class);
        register(kryo, BigDecimal.class);
        register(kryo, BigInteger.class);
        register(kryo, Date.class);
        register(kryo, Time.class);
        register(kryo, Timestamp.class);
        register(kryo, java.util.Date.class);
        register(kryo, UUID.class);
        register(kryo, URL.class);
        register(kryo, SerialBlob.class);
        register(kryo, SerialClob.class);
        register(kryo, SerialArray.class);
        register(kryo, SerialRef.class);

        register(kryo, byte[].class);
        register(kryo, int[].class);
        register(kryo, long[].class);
        register(kryo, String[].class);
        register(kryo, Object[].class);
        register(kryo, Serializable[].class);
        register(kryo, LobRef.class);
        register(kryo, LobMsg.class);
    }

    static boolean isAllowed(String className) {
        if (className == null) {
            return false;
        }
        String normalized = className;
        while (normalized.startsWith("[")) {
            normalized = normalized.substring(1);
        }
        if (normalized.length() == 1) {
            return "B".equals(normalized) || "C".equals(normalized) || "D".equals(normalized)
                    || "F".equals(normalized) || "I".equals(normalized) || "J".equals(normalized)
                    || "S".equals(normalized) || "Z".equals(normalized);
        }
        if (normalized.startsWith("L") && normalized.endsWith(";")) {
            normalized = normalized.substring(1, normalized.length() - 1);
        }
        return NAMES.contains(normalized);
    }

    private static void register(Kryo kryo, Class<?> type) {
        kryo.register(type);
        NAMES.add(type.getName());
    }

    private static Class<?> colInfoClass() {
        try {
            return Class.forName("com.jdbcagent.core.support.SerialRowSetMetaDataImpl$ColInfo");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    private static Class<?> colInfoArrayClass() {
        try {
            return Class.forName("[Lcom.jdbcagent.core.support.SerialRowSetMetaDataImpl$ColInfo;");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }
}
