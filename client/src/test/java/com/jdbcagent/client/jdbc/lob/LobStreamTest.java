package com.jdbcagent.client.jdbc.lob;

import com.jdbcagent.client.JdbcAgentConnector;
import com.jdbcagent.core.protocol.Ack;
import com.jdbcagent.core.protocol.LobMsg;
import com.jdbcagent.core.protocol.LobRef;
import com.jdbcagent.core.protocol.Packet;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.Reader;
import java.sql.Types;

public class LobStreamTest {

    @Test
    public void binaryStreamReadsChunksAndCloseReleases() throws Exception {
        byte[] data = new byte[10];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }
        ScriptedConnector connector = new ScriptedConnector(data);
        LobRef ref = new LobRef(7, Types.BLOB, data.length, 4);
        LobInputStream input = new LobInputStream(connector, ref, true);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[3];
        int n;
        while ((n = input.read(buffer)) >= 0) {
            out.write(buffer, 0, n);
        }
        input.close();
        Assert.assertArrayEquals(data, out.toByteArray());
        Assert.assertEquals(1, connector.closes);
        Assert.assertTrue(connector.reads >= 3);
    }

    @Test
    public void characterStreamDecodesUtf8() throws Exception {
        String text = "中a文";
        ScriptedConnector connector = new ScriptedConnector(text.getBytes("UTF-8"));
        LobRef ref = new LobRef(8, Types.CLOB, text.length(), 2);
        Reader reader = new LobReader(connector, ref, true);
        StringBuilder builder = new StringBuilder();
        char[] buffer = new char[8];
        int n;
        while ((n = reader.read(buffer)) >= 0) {
            builder.append(buffer, 0, n);
        }
        reader.close();
        Assert.assertEquals(text, builder.toString());
        Assert.assertEquals(1, connector.closes);
    }

    private static final class ScriptedConnector implements JdbcAgentConnector {
        private final byte[] data;
        private int closes;
        private int reads;

        private ScriptedConnector(byte[] data) {
            this.data = data;
        }

        @Override
        public byte[] write(Packet packet) {
            if (packet.getType() == Packet.PacketType.LOB_CLOSE) {
                closes++;
                return Packet.newBuilder(packet.getId())
                        .setType(Packet.PacketType.ACK)
                        .setBody(new Ack())
                        .build()
                        .toByteArray();
            }
            reads++;
            LobMsg request = (LobMsg) packet.getMessage();
            int position = request.getPosition().intValue();
            int length = request.getLength();
            int from = Math.min(position, data.length);
            int to = Math.min(data.length, from + length);
            byte[] slice = new byte[to - from];
            System.arraycopy(data, from, slice, 0, slice.length);
            return Packet.newBuilder(packet.getId())
                    .setType(Packet.PacketType.LOB_READ)
                    .setBody(LobMsg.newBuilder()
                            .setLobId(request.getLobId())
                            .setPosition(request.getPosition())
                            .setData(slice)
                            .setEof(to >= data.length)
                            .build())
                    .build()
                    .toByteArray();
        }

        @Override
        public void connect() {
        }

        @Override
        public void disconnect() {
        }

        @Override
        public void start() {
        }

        @Override
        public void stop() {
        }
    }
}
