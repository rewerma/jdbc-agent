package com.jdbcagent.client.jdbc.lob;

import com.jdbcagent.client.JdbcAgentConnector;
import com.jdbcagent.core.protocol.LobRef;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

public class StreamingBlob implements Blob {
    private final JdbcAgentConnector connector;
    private final LobRef ref;
    private boolean freed;

    public StreamingBlob(JdbcAgentConnector connector, LobRef ref) {
        this.connector = connector;
        this.ref = ref;
    }

    @Override
    public long length() throws SQLException {
        ensureOpen();
        return ref.getLength();
    }

    @Override
    public byte[] getBytes(long pos, int length) throws SQLException {
        ensureOpen();
        if (pos < 1) {
            throw new SQLException("invalid blob position: " + pos);
        }
        InputStream input = new LobInputStream(connector, ref, false, pos - 1);
        try {
            byte[] buffer = new byte[length];
            int offset = 0;
            while (offset < length) {
                int n = input.read(buffer, offset, length - offset);
                if (n < 0) {
                    break;
                }
                offset += n;
            }
            if (offset == length) {
                return buffer;
            }
            byte[] exact = new byte[offset];
            System.arraycopy(buffer, 0, exact, 0, offset);
            return exact;
        } catch (IOException e) {
            throw new SQLException(e);
        } finally {
            try {
                input.close();
            } catch (IOException ignored) {
                // 这次读取不释放服务端文件
            }
        }
    }

    @Override
    public InputStream getBinaryStream() {
        return new LobInputStream(connector, ref, true);
    }

    @Override
    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        ensureOpen();
        if (pos < 1) {
            throw new SQLException("invalid blob position: " + pos);
        }
        final InputStream input = new LobInputStream(connector, ref, true, pos - 1);
        return new InputStream() {
            private long remaining = length;

            @Override
            public int read() throws IOException {
                if (remaining <= 0) {
                    return -1;
                }
                int value = input.read();
                if (value >= 0) {
                    remaining--;
                }
                return value;
            }

            @Override
            public int read(byte[] buffer, int off, int len) throws IOException {
                if (remaining <= 0) {
                    return -1;
                }
                int n = input.read(buffer, off, (int) Math.min(len, remaining));
                if (n > 0) {
                    remaining -= n;
                }
                return n;
            }

            @Override
            public void close() throws IOException {
                input.close();
            }
        };
    }

    @Override
    public long position(byte[] pattern, long start) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public long position(Blob pattern, long start) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public OutputStream setBinaryStream(long pos) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void truncate(long len) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void free() throws SQLException {
        if (freed) {
            return;
        }
        freed = true;
        LobChannel.close(connector, ref);
    }

    private void ensureOpen() throws SQLException {
        if (freed) {
            throw new SQLException("blob is freed");
        }
    }

    public static byte[] readAll(InputStream input) throws SQLException {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buffer = new byte[8192];
            int n;
            while ((n = input.read(buffer)) >= 0) {
                out.write(buffer, 0, n);
            }
            return out.toByteArray();
        } catch (IOException e) {
            throw new SQLException(e);
        } finally {
            try {
                input.close();
            } catch (IOException ignored) {
                // 读完后的关闭失败不影响已经拿到的字节
            }
        }
    }
}
