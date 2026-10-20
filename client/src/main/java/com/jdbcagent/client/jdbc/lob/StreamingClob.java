package com.jdbcagent.client.jdbc.lob;

import com.jdbcagent.client.JdbcAgentConnector;
import com.jdbcagent.core.protocol.LobRef;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

public class StreamingClob implements Clob {
    private final JdbcAgentConnector connector;
    private final LobRef ref;
    private boolean freed;

    public StreamingClob(JdbcAgentConnector connector, LobRef ref) {
        this.connector = connector;
        this.ref = ref;
    }

    @Override
    public long length() throws SQLException {
        ensureOpen();
        return ref.getLength();
    }

    @Override
    public String getSubString(long pos, int length) throws SQLException {
        ensureOpen();
        if (pos < 1) {
            throw new SQLException("invalid clob position: " + pos);
        }
        Reader reader = new LobReader(connector, ref, false);
        try {
            skipFully(reader, pos - 1);
            char[] buffer = new char[length];
            int offset = 0;
            while (offset < length) {
                int n = reader.read(buffer, offset, length - offset);
                if (n < 0) {
                    break;
                }
                offset += n;
            }
            return new String(buffer, 0, offset);
        } catch (IOException e) {
            throw new SQLException(e);
        } finally {
            try {
                reader.close();
            } catch (IOException ignored) {
                // 这次读取不释放服务端文件
            }
        }
    }

    @Override
    public Reader getCharacterStream() {
        return new LobReader(connector, ref, true);
    }

    @Override
    public Reader getCharacterStream(long pos, long length) throws SQLException {
        ensureOpen();
        if (pos < 1) {
            throw new SQLException("invalid clob position: " + pos);
        }
        final Reader reader = new LobReader(connector, ref, true);
        try {
            skipFully(reader, pos - 1);
        } catch (IOException e) {
            closeQuietly(reader);
            throw new SQLException(e);
        }
        return new Reader() {
            private long remaining = length;

            @Override
            public int read(char[] cbuf, int off, int len) throws IOException {
                if (remaining <= 0) {
                    return -1;
                }
                int n = reader.read(cbuf, off, (int) Math.min(len, remaining));
                if (n > 0) {
                    remaining -= n;
                }
                return n;
            }

            @Override
            public void close() throws IOException {
                reader.close();
            }
        };
    }

    @Override
    public InputStream getAsciiStream() {
        return new LobInputStream(connector, ref, true);
    }

    @Override
    public long position(String searchstr, long start) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public long position(Clob searchstr, long start) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int setString(long pos, String str) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int setString(long pos, String str, int offset, int len) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public OutputStream setAsciiStream(long pos) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Writer setCharacterStream(long pos) throws SQLException {
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

    protected JdbcAgentConnector connector() {
        return connector;
    }

    protected LobRef ref() {
        return ref;
    }

    private void ensureOpen() throws SQLException {
        if (freed) {
            throw new SQLException("clob is freed");
        }
    }

    public static String readAll(Reader reader) throws SQLException {
        try {
            StringBuilder builder = new StringBuilder();
            char[] buffer = new char[8192];
            int n;
            while ((n = reader.read(buffer)) >= 0) {
                builder.append(buffer, 0, n);
            }
            return builder.toString();
        } catch (IOException e) {
            throw new SQLException(e);
        } finally {
            try {
                reader.close();
            } catch (IOException ignored) {
                // 读完后的关闭失败不影响已经拿到的文本
            }
        }
    }

    private static void skipFully(Reader reader, long count) throws IOException {
        long left = count;
        while (left > 0) {
            long skipped = reader.skip(left);
            if (skipped > 0) {
                left -= skipped;
                continue;
            }
            if (reader.read() < 0) {
                return;
            }
            left--;
        }
    }

    private static void closeQuietly(Reader reader) {
        try {
            reader.close();
        } catch (IOException ignored) {
            // 定位失败时尽力关闭
        }
    }
}
