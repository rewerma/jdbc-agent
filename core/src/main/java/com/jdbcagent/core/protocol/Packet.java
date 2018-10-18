package com.jdbcagent.core.protocol;


import com.jdbcagent.core.util.serialize.JavaSerializeUtil;
import com.jdbcagent.core.util.serialize.KryoSerializeUtil;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * JDBC-Agent protocol Packet
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class Packet implements Serializable {
    private static final long serialVersionUID = 3848315821658610391L;

    private static final AtomicLong packetId = new AtomicLong();

    private Long id;
    private Message message;
    private PacketType type;
    private int version = 1;


    public Packet() {
    }

    public Packet(Long id) {
        this.id = id;
    }

    public static Builder newBuilder() {
        return new Builder(new Packet());
    }

    public static Builder newBuilder(long id) {
        return new Builder(new Packet(id));
    }

    public byte[] toByteArray(SerializeType serializeType) {
        if (serializeType == SerializeType.kryo) {
            return KryoSerializeUtil.serialize(this);
        } else {
            return JavaSerializeUtil.serialize(this);
        }
    }

    public static Packet parse(byte[] bytes, SerializeType serializeType) {
        if (serializeType == SerializeType.kryo) {
            return (Packet) KryoSerializeUtil.deserialize(bytes);
        } else {
            return (Packet) JavaSerializeUtil.deserialize(bytes);
        }
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }

    public PacketType getType() {
        return type;
    }

    public void setType(PacketType type) {
        this.type = type;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public Message getBody() throws SQLException {
        if (message == null) {
            return null;
        } else {
            if (message instanceof Ack) {
                Ack ack = (Ack) message;
                if (ack.getErrorCode() == 0) {
                    return ack;
                } else {
                    throw new SQLException(ack.getErrorMessage());
                }
            } else {
                return message;
            }
        }
    }

    public Ack getAck() throws SQLException {
        if (message != null) {
            if (message instanceof Ack) {
                Ack ack = (Ack) message;
                if (ack.getErrorCode() == 0) {
                    return ack;
                } else {
                    if (ack.getErrorMessage() != null) {
                        throw new SQLException(ack.getErrorMessage());
                    } else {
                        throw new SQLException("error ack from jdbc agent server ");
                    }
                }
            }
        }
        throw new SQLException("no ack from jdbc agent server ");
    }

    public static class Builder {
        private Packet packet;

        public Builder(Packet packet) {
            this.packet = packet;
        }

        public Builder incrementAndGetId() {
            packet.setId(packetId.incrementAndGet());
            return this;
        }

        public Builder setType(PacketType packetType) {
            packet.setType(packetType);
            return this;
        }

        public Builder setBody(Message message) {
            packet.setMessage(message);
            return this;
        }

        public Builder setVersion(int version) {
            packet.setVersion(version);
            return this;
        }

        public Packet build() {
            return packet;
        }
    }

    public enum SerializeType {
        java, kryo
    }

    public enum PacketType {
        ACK,
        CLIENT_AUTH,

        CONN_CONNECT,
        CONN_CLOSE,
        CONN_METHOD,
        CONN_SERIAL_METHOD,

        STMT_CLOSE,
        STMT_METHOD,

        PRE_STMT_METHOD,
        PRE_STMT_CLOSE,

        CLA_STMT_METHOD,
        CLA_STMT_CLOSE,

        DB_METADATA_METHOD,

        RS_CLOSE,
        RS_META_DATA,
        RS_FETCH_ROWS
    }
}
