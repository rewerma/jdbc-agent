package com.jdbcagent.client.jdbc.lob;

import com.jdbcagent.client.JdbcAgentConnector;
import com.jdbcagent.core.protocol.LobMsg;
import com.jdbcagent.core.protocol.LobRef;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

/**
 * 按块从服务端拉取大字段字节。
 */
public class LobInputStream extends InputStream {
    private final JdbcAgentConnector connector;
    private final LobRef ref;
    private final boolean releaseOnClose;
    private byte[] chunk = new byte[0];
    private int offset;
    private long position;
    private boolean eof;
    private boolean closed;

    public LobInputStream(JdbcAgentConnector connector, LobRef ref, boolean releaseOnClose) {
        this(connector, ref, releaseOnClose, 0L);
    }

    public LobInputStream(JdbcAgentConnector connector, LobRef ref, boolean releaseOnClose, long startPosition) {
        this.connector = connector;
        this.ref = ref;
        this.releaseOnClose = releaseOnClose;
        this.position = startPosition;
    }

    @Override
    public int read() throws IOException {
        byte[] one = new byte[1];
        int n = read(one, 0, 1);
        return n < 0 ? -1 : one[0] & 0xff;
    }

    @Override
    public int read(byte[] buffer, int off, int len) throws IOException {
        if (closed) {
            throw new IOException("stream closed");
        }
        if (len == 0) {
            return 0;
        }
        fill();
        if (offset >= chunk.length) {
            return -1;
        }
        int n = Math.min(len, chunk.length - offset);
        System.arraycopy(chunk, offset, buffer, off, n);
        offset += n;
        return n;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        chunk = new byte[0];
        if (!releaseOnClose) {
            return;
        }
        try {
            LobChannel.close(connector, ref);
        } catch (SQLException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    private void fill() throws IOException {
        if (offset < chunk.length || eof) {
            return;
        }
        try {
            int size = ref.getChunkSize() > 0 ? ref.getChunkSize() : 1024 * 1024;
            LobMsg message = LobChannel.read(connector, ref, position, size);
            byte[] data = message.getData() == null ? new byte[0] : message.getData();
            chunk = data;
            offset = 0;
            position += data.length;
            eof = message.isEof() || data.length == 0;
        } catch (SQLException e) {
            throw new IOException(e.getMessage(), e);
        }
    }
}
