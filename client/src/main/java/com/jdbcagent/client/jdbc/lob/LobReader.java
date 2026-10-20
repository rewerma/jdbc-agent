package com.jdbcagent.client.jdbc.lob;

import com.jdbcagent.client.JdbcAgentConnector;
import com.jdbcagent.core.protocol.LobRef;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;

/**
 * 把分块字节按 UTF-8 解码成字符流。服务端按完整字符写入，块之间不会拆开字符。
 */
public class LobReader extends Reader {
    private final Reader delegate;

    public LobReader(JdbcAgentConnector connector, LobRef ref, boolean releaseOnClose) {
        this.delegate = new InputStreamReader(
                new LobInputStream(connector, ref, releaseOnClose),
                Charset.forName("UTF-8"));
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
        return delegate.read(cbuf, off, len);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
