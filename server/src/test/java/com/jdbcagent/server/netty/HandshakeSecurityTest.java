package com.jdbcagent.server.netty;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.jdbcagent.core.protocol.ClientAuth;
import com.jdbcagent.core.protocol.Packet;
import com.jdbcagent.core.protocol.PreparedStatementMsg;
import com.jdbcagent.core.support.SerialRowSetImpl;
import com.jdbcagent.core.util.SerializeUtil;
import com.jdbcagent.server.config.DataSourceConf;
import com.jdbcagent.server.config.JdbcAgentConf;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class HandshakeSecurityTest {

    @Test
    public void rejectsClassOutsideProtocol() throws Exception {
        Kryo kryo = new Kryo();
        kryo.setRegistrationRequired(false);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output output = new Output(baos);
        kryo.writeClassAndObject(output, new File("/tmp/jdbc-agent"));
        output.close();

        try {
            SerializeUtil.deserialize(baos.toByteArray());
            Assert.fail("disallowed class should not be deserialized");
        } catch (RuntimeException e) {
            Assert.assertTrue(rootMessage(e).contains("not allowed"));
        }
    }

    @Test
    public void roundTripsProtocolPackets() throws Exception {
        Packet auth = Packet.newBuilder()
                .incrementAndGetId()
                .setType(Packet.PacketType.CLIENT_AUTH)
                .setBody(ClientAuth.newBuilder().setUsername("test").setPassword("123456").build())
                .build();
        Packet parsedAuth = (Packet) SerializeUtil.deserialize(SerializeUtil.serialize(auth));
        ClientAuth clientAuth = (ClientAuth) parsedAuth.getMessage();
        Assert.assertEquals("test", clientAuth.getUsername());

        LinkedList<String> values = new LinkedList<String>();
        values.add("row");
        PreparedStatementMsg statementMsg = PreparedStatementMsg.newBuilder()
                .setId(1L)
                .setMethod(PreparedStatementMsg.Method.executeQuery)
                .setParams(new Serializable[]{"sql", Integer.valueOf(1), values})
                .build();
        Packet statement = Packet.newBuilder()
                .incrementAndGetId()
                .setType(Packet.PacketType.PRE_STMT_METHOD)
                .setBody(statementMsg)
                .build();
        Packet parsedStatement = (Packet) SerializeUtil.deserialize(SerializeUtil.serialize(statement));
        Assert.assertEquals(Packet.PacketType.PRE_STMT_METHOD, parsedStatement.getType());

        SerialRowSetImpl rowSet = new SerialRowSetImpl();
        byte[] rowSetBytes = SerializeUtil.serialize(rowSet);
        Assert.assertTrue(SerializeUtil.deserialize(rowSetBytes) instanceof SerialRowSetImpl);
    }

    @Test
    public void authenticateMatchesConfiguredAccount() {
        JdbcAgentConf conf = new JdbcAgentConf();
        JdbcAgentConf.JdbcAgent agent = new JdbcAgentConf.JdbcAgent();
        agent.setCatalog("mytest");
        DataSourceConf dataSourceConf = new DataSourceConf();
        dataSourceConf.setAccessUsername("test");
        dataSourceConf.setAccessPassword("123456");
        Map<String, Object> dsManager = new HashMap<String, Object>();
        dsManager.put("driverClass", "org.h2.Driver");
        dsManager.put("jdbcUrl", "jdbc:h2:mem:auth");
        dsManager.put("username", "sa");
        dsManager.put("password", "");
        dataSourceConf.setDsManager(dsManager);
        agent.setDataSources(java.util.Collections.singletonList(dataSourceConf));
        conf.setJdbcAgent(agent);
        conf.init();

        Assert.assertTrue(JdbcAgentConf.authenticate("test", "123456"));
        Assert.assertFalse(JdbcAgentConf.authenticate("test", "wrong"));
        Assert.assertFalse(JdbcAgentConf.authenticate("", ""));
        Assert.assertFalse(JdbcAgentConf.authenticate(null, "123456"));
    }

    private static String rootMessage(Throwable error) {
        Throwable current = error;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current.getMessage() == null ? "" : current.getMessage();
    }
}
