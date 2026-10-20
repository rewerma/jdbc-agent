package com.jdbcagent.client.jdbc.lob;

import com.jdbcagent.client.JdbcAgentConnector;
import com.jdbcagent.core.protocol.LobMsg;
import com.jdbcagent.core.protocol.LobRef;
import com.jdbcagent.core.protocol.Packet;

import java.sql.SQLException;

final class LobChannel {
    private LobChannel() {
    }

    static LobMsg read(JdbcAgentConnector connector, LobRef ref, long position, int length) throws SQLException {
        synchronized (connector) {
            Packet response = Packet.parse(connector.write(Packet.newBuilder()
                    .incrementAndGetId()
                    .setType(Packet.PacketType.LOB_READ)
                    .setBody(LobMsg.newBuilder()
                            .setLobId(ref.getLobId())
                            .setPosition(position)
                            .setLength(length)
                            .build())
                    .build()));
            return (LobMsg) response.getBody();
        }
    }

    static void close(JdbcAgentConnector connector, LobRef ref) throws SQLException {
        synchronized (connector) {
            Packet response = Packet.parse(connector.write(Packet.newBuilder()
                    .incrementAndGetId()
                    .setType(Packet.PacketType.LOB_CLOSE)
                    .setBody(LobMsg.newBuilder().setLobId(ref.getLobId()).build())
                    .build()));
            response.getAck();
        }
    }
}
