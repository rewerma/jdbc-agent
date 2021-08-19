package com.jdbcagent.server.test;

import com.jdbcagent.core.protocol.*;
import com.jdbcagent.core.protocol.Packet.PacketType;
import com.jdbcagent.server.config.ConfigParser;
import com.jdbcagent.server.config.JdbcAgentConf;
import com.jdbcagent.server.netty.JdbcAgentNettyServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import javax.sql.RowSet;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.sql.*;
import java.util.Date;
import java.util.LinkedList;


public class JdbcAgentServerTest {
    private final ByteBuffer header = ByteBuffer.allocate(4);

    private JdbcAgentNettyServer jdbcAgentServer;
    private SocketChannel channel;
    private JdbcAgentConf jdbcAgentConf;

    @Before
    public void setUp() throws Exception {
        jdbcAgentServer = JdbcAgentNettyServer.instance();
        InputStream in = JdbcAgentServerTest.class.getClassLoader().getResourceAsStream("jdbc-agent.yml");
        jdbcAgentConf = ConfigParser.parse(in);
        jdbcAgentConf.init();
        jdbcAgentServer.setJdbcAgentConf(jdbcAgentConf);
        jdbcAgentServer.start();

        ddl();

        channel = SocketChannel.open();
    }

    @Test
    public void testAll() throws Exception {
        testAuth();
        long connId = testConnection();
        long stmtId = testStatement(connId);
        testUpdate(stmtId);
        long rsId = testResultSet(stmtId);
        RowSet rs = testRowSet(rsId);
        while (rs.next()) {
            System.out.println(rs.getLong("id") + " "
                    + rs.getString("name") + " "
                    + rs.getInt("gender") + " "
                    + rs.getString("email") + " "
                    + rs.getTimestamp("sys_time")
            );
        }
        rs.close();

        System.out.println();

        long pstatId = testPreparedStatement(connId);
        LinkedList<PreparedStatementMsg> paramsQueue = setPStmtParam(pstatId, 2L);
        rsId = testExePrepariedStatement(pstatId, paramsQueue);
        rs = testRowSet(rsId);
        while (rs.next()) {
            System.out.println(rs.getLong("id") + " "
                    + rs.getString("name") + " "
                    + rs.getInt("gender") + " "
                    + rs.getString("email") + " "
                    + rs.getTimestamp("sys_time")
            );
        }
        rs.close();
    }

    @After
    public void tearDown() throws IOException {
        channel.close();

        jdbcAgentServer.stop();
    }

    private void testAuth() throws Exception {
        channel.connect(new InetSocketAddress("127.0.0.1", jdbcAgentConf.getJdbcAgent().getPort()));

        writeWithHeader(channel,
                Packet.newBuilder()
                        .incrementAndGetId()
                        .setType(PacketType.CLIENT_AUTH)
                        .setBody(ClientAuth.newBuilder()
                                .setUsername("")
                                .setPassword("")
                                .setNetReadTimeout(10000)
                                .setNetWriteTimeout(10000)
                                .build()).build().toByteArray());

        Packet p = Packet.parse(readNextPacket(channel));

        if (p.getType() != PacketType.CLIENT_AUTH) {
            throw new Exception("unexpected packet type when ack is expected");
        }
    }

    private Long testConnection() throws Exception {
        writeWithHeader(channel, Packet
                .newBuilder()
                .incrementAndGetId()
                .setType(PacketType.CONN_CONNECT)
                .setBody(ConnectionMsg
                        .newBuilder()
                        .setCatalog("mytest")
                        .setUsername("test")
                        .setPassword("123456").build())
                .build().toByteArray());

        Packet p = Packet.parse(readNextPacket(channel));

        ConnectionMsg connectMsg = (ConnectionMsg) p.getBody();
        Long connId = connectMsg.getId();
        Assert.assertNotNull(connId);
        return connId;
    }

    private Long testStatement(long connId) throws Exception {
        writeWithHeader(channel,
                Packet.newBuilder()
                        .incrementAndGetId()
                        .setType(PacketType.CONN_METHOD)
                        .setBody(ConnectionMsg.newBuilder()
                                .setId(connId)
                                .setMethod(ConnectionMsg.Method.createStatement)
                                .setParams(new Serializable[0])
                                .build()).build().toByteArray());
        Packet p = Packet.parse(readNextPacket(channel));
        ConnectionMsg connectionMsg = (ConnectionMsg) p.getBody();
        Long stmtId = (Long) connectionMsg.getResponse();
        Assert.assertNotNull(stmtId);
        return stmtId;
    }

    private Integer testUpdate(long stmtId) throws Exception {
        String sql = "UPDATE `t_user` SET `name`='test_0' WHERE `id`=1";
        writeWithHeader(channel,
                Packet.newBuilder()
                        .incrementAndGetId()
                        .setType(PacketType.STMT_METHOD)
                        .setBody(StatementMsg.newBuilder().setId(stmtId)
                                .setMethod(StatementMsg.Method.executeUpdate)
                                .setParams(new Serializable[]{sql})
                                .build()).build().toByteArray());
        Packet p = Packet.parse(readNextPacket(channel));
        StatementMsg statementMsg = (StatementMsg) p.getBody();
        Integer updateCount = (Integer) statementMsg.getResponse();
        Assert.assertEquals(updateCount, new Integer(1));
        return updateCount;
    }

    private Long testResultSet(long stmtId) throws Exception {
        String sql = "SELECT * FROM  `t_user` WHERE `id`!=0";
        writeWithHeader(channel,
                Packet.newBuilder()
                        .incrementAndGetId()
                        .setType(PacketType.STMT_METHOD)
                        .setBody(StatementMsg.newBuilder().setId(stmtId)
                                .setMethod(StatementMsg.Method.executeQuery)
                                .setParams(new Serializable[]{sql})
                                .build()).build().toByteArray());
        Packet p = Packet.parse(readNextPacket(channel));
        StatementMsg statementMsg = (StatementMsg) p.getBody();
        Long rsId = (Long) statementMsg.getResponse();
        Assert.assertNotNull(rsId);
        return rsId;
    }

    private RowSet testRowSet(long rsId) throws Exception {
        writeWithHeader(channel,
                Packet.newBuilder()
                        .incrementAndGetId()
                        .setType(PacketType.RS_FETCH_ROWS)
                        .setBody(ResultSetMsg.newBuilder().setId(rsId)
                                .setBatchSize(500)
                                .build()).build().toByteArray());
        Packet p = Packet.parse(readNextPacket(channel));
        ResultSetMsg resultSetMsg = (ResultSetMsg) p.getBody();
        RowSet rowSet = resultSetMsg.getRowSet();
        Assert.assertNotNull(rowSet);
        return rowSet;
    }

    private Long testPreparedStatement(long connId) throws Exception {
        String sql = "SELECT * FROM  `t_user` WHERE `id`=?";
        writeWithHeader(channel,
                Packet.newBuilder()
                        .incrementAndGetId()
                        .setType(PacketType.CONN_METHOD)
                        .setBody(ConnectionMsg.newBuilder()
                                .setId(connId)
                                .setMethod(ConnectionMsg.Method.prepareStatement)
                                .setParams(new Serializable[]{sql})
                                .build()).build().toByteArray());
        Packet p = Packet.parse(readNextPacket(channel));
        ConnectionMsg connectionMsg = (ConnectionMsg) p.getBody();
        Long pstmtId = (Long) connectionMsg.getResponse();
        Assert.assertNotNull(pstmtId);
        return pstmtId;
    }

    private LinkedList<PreparedStatementMsg> setPStmtParam(long pstmtId, long testId) {
        LinkedList<PreparedStatementMsg> paramsQueue = new LinkedList<>();
        PreparedStatementMsg.Builder builder = PreparedStatementMsg.newBuilder().setId(pstmtId)
                .setParamType(PreparedStatementMsg.ParamType.LONG).setParameterIndex(1);

        PreparedStatementMsg preparedStatementMsg = builder.setParams(new Serializable[]{testId}).build();

        paramsQueue.offer(preparedStatementMsg);
        return paramsQueue;
    }

    private Long testExePrepariedStatement(long pstmtId, LinkedList<PreparedStatementMsg> paramsQueue) throws Exception {
        writeWithHeader(channel,
                Packet.newBuilder()
                        .incrementAndGetId()
                        .setType(PacketType.PRE_STMT_METHOD)
                        .setBody(PreparedStatementMsg.newBuilder().setId(pstmtId)
                                .setMethod(PreparedStatementMsg.Method.executeQuery)
                                .setParams(new Serializable[]{paramsQueue}).build())
                        .build().toByteArray());
        Packet p = Packet.parse(readNextPacket(channel));
        PreparedStatementMsg preparedStatementMsg = (PreparedStatementMsg) p.getBody();
        Long rsId = (Long) preparedStatementMsg.getResponse();
        Assert.assertNotNull(rsId);
        return rsId;
    }

    public static void ddl() throws SQLException {
        String create_tb_sql = "CREATE TABLE `t_user` (\n" +
                "  `id` bigint(20) NOT NULL,\n" +
                "  `name` varchar(50) NOT NULL,\n" +
                "  `gender` int(11) DEFAULT NULL,\n" +
                "  `email` varchar(200) DEFAULT NULL,\n" +
                "  `sys_time` datetime NOT NULL,\n" +
                "  PRIMARY KEY (`id`)\n" +
                ")";
        DataSource ds = JdbcAgentConf.getDataSource("mytest|test|123456");
        Connection conn = ds.getConnection();
        Statement stmt = conn.createStatement();
        stmt.executeUpdate(create_tb_sql);
        stmt.close();

        String insert_sql = "INSERT INTO `t_user` VALUES (?,?,?,?,?)";
        for (int i = 1; i <= 5; i++) {
            PreparedStatement pstmt = conn.prepareStatement(insert_sql);
            pstmt.setLong(1, i);
            pstmt.setString(2, "test_" + i);
            pstmt.setInt(3, 20 + i);
            pstmt.setString(4, "test" + i + "@gmail.com");
            pstmt.setTimestamp(5, new Timestamp(new Date().getTime()));
            pstmt.executeUpdate();
            pstmt.close();
        }

        conn.close();
    }

    private byte[] readNextPacket(SocketChannel channel) throws IOException {
        header.clear();
        read(channel, header);
        int bodyLen = header.getInt(0);
        ByteBuffer bodyBuf = ByteBuffer.allocate(bodyLen);
        read(channel, bodyBuf);
        return bodyBuf.array();
    }

    private void writeWithHeader(SocketChannel channel, byte[] body) throws IOException {
        ByteBuffer header = ByteBuffer.allocate(4);
        header.putInt(body.length);
        header.flip();
        int len = channel.write(header);
        assert (len == header.capacity());

        channel.write(ByteBuffer.wrap(body));
    }

    private void read(SocketChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int r = channel.read(buffer);
            if (r == -1) {
                throw new IOException("end of stream when reading header");
            }
        }
    }
}
