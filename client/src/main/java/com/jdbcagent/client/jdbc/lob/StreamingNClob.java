package com.jdbcagent.client.jdbc.lob;

import com.jdbcagent.client.JdbcAgentConnector;
import com.jdbcagent.core.protocol.LobRef;

import java.sql.NClob;

public class StreamingNClob extends StreamingClob implements NClob {
    public StreamingNClob(JdbcAgentConnector connector, LobRef ref) {
        super(connector, ref);
    }
}
