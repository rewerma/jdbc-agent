package com.jdbcagent.server.jdbc;

import com.jdbcagent.core.protocol.ConnectionMsg;
import com.jdbcagent.server.config.DataSourceConf;
import com.jdbcagent.server.config.JdbcAgentConf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ConnectionReleaseTest {
    private static final Integer CHANNEL_ID = 42;

    @Before
    public void setUp() {
        JdbcAgentConf conf = new JdbcAgentConf();
        JdbcAgentConf.JdbcAgent agent = new JdbcAgentConf.JdbcAgent();
        agent.setCatalog("release_db");
        DataSourceConf dataSourceConf = new DataSourceConf();
        dataSourceConf.setAccessUsername("release_user");
        dataSourceConf.setAccessPassword("release_pass");
        dataSourceConf.setMaxConnections(2);
        Map<String, Object> dsManager = new HashMap<String, Object>();
        dsManager.put("driverClass", "org.h2.Driver");
        dsManager.put("jdbcUrl", "jdbc:h2:mem:conn_release;DB_CLOSE_DELAY=-1");
        dsManager.put("username", "sa");
        dsManager.put("password", "");
        dataSourceConf.setDsManager(dsManager);
        agent.setDataSources(Collections.singletonList(dataSourceConf));
        conf.setJdbcAgent(agent);
        conf.init();
    }

    @Test
    public void releaseFreesOneConnectionAndChannelCloseFreesTheRest() throws Exception {
        ConnectionServer first = connect();
        ConnectionServer second = connect();
        long firstId = idOf(first);
        long secondId = idOf(second);

        Assert.assertNotNull(ConnectionServer.CONNECTIONS.get(firstId));
        Assert.assertNotNull(ConnectionServer.CONNECTIONS.get(secondId));

        first.connMethod(ConnectionMsg.newBuilder().setMethod(ConnectionMsg.Method.release).build());
        Assert.assertNull(ConnectionServer.CONNECTIONS.get(firstId));
        Assert.assertNotNull(ConnectionServer.CONNECTIONS.get(secondId));

        ConnectionServer third = connect();
        long thirdId = idOf(third);
        Assert.assertNotNull(ConnectionServer.CONNECTIONS.get(thirdId));

        ConnectionServer.close(CHANNEL_ID);
        Assert.assertNull(ConnectionServer.CONNECTIONS.get(secondId));
        Assert.assertNull(ConnectionServer.CONNECTIONS.get(thirdId));

        ConnectionServer again = connect();
        ConnectionServer againToo = connect();
        again.connMethod(ConnectionMsg.newBuilder().setMethod(ConnectionMsg.Method.release).build());
        againToo.connMethod(ConnectionMsg.newBuilder().setMethod(ConnectionMsg.Method.release).build());
    }

    @Test
    public void failedConnectRemovesRegistration() throws Exception {
        ConnectionServer connectionServer = new ConnectionServer();
        try {
            connectionServer.connect(CHANNEL_ID, "release_db", "release_user", "wrong");
            Assert.fail("connect should fail");
        } catch (SQLException expected) {
            Assert.assertTrue(expected.getMessage().contains("username or password"));
        }
        long id = currentId(connectionServer);
        Assert.assertNull(ConnectionServer.CONNECTIONS.get(id));
        Assert.assertFalse(channelContains(CHANNEL_ID, id));
    }

    private static ConnectionServer connect() throws Exception {
        ConnectionServer connectionServer = new ConnectionServer();
        connectionServer.connect(CHANNEL_ID, "release_db", "release_user", "release_pass");
        return connectionServer;
    }

    private static long idOf(ConnectionServer connectionServer) throws Exception {
        return connectionServer.connect(CHANNEL_ID, "release_db", "release_user", "release_pass");
    }

    private static long currentId(ConnectionServer connectionServer) throws Exception {
        Field field = ConnectionServer.class.getDeclaredField("currentId");
        field.setAccessible(true);
        return field.getLong(connectionServer);
    }

    @SuppressWarnings("unchecked")
    private static boolean channelContains(Integer channelId, long connectionId) throws Exception {
        Field field = ConnectionServer.class.getDeclaredField("CHANNEL_CONNECTIONS");
        field.setAccessible(true);
        Map<Integer, Set<Long>> channels = (Map<Integer, Set<Long>>) field.get(null);
        Set<Long> ids = channels.get(channelId);
        return ids != null && ids.contains(connectionId);
    }
}
