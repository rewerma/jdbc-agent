package com.jdbcagent.server.jdbc;

import com.jdbcagent.core.protocol.LobRef;
import com.jdbcagent.core.protocol.LobTypes;
import com.jdbcagent.core.support.SerialRowSetImpl;
import com.jdbcagent.core.util.SerializeUtil;
import org.junit.Assert;
import org.junit.Test;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import java.io.ByteArrayInputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;

public class LargeColumnTest {

    @Test
    public void knownLengthWithinLimitStaysInRowSet() throws Exception {
        byte[] blob = new byte[]{1, 2, 3, 4};
        String text = "small-clob";
        ResultSetServer server = load("select b, c from lob_t where id = 1", blob, text);
        try {
            SerialRowSetImpl rowSet = (SerialRowSetImpl) server.fetchRows(10);
            Assert.assertTrue(rowSet.next());
            Assert.assertTrue(rowSet.getObject(1) instanceof SerialBlob);
            Assert.assertArrayEquals(blob, ((SerialBlob) rowSet.getObject(1)).getBytes(1, blob.length));
            Assert.assertTrue(rowSet.getObject(2) instanceof SerialClob);
            Assert.assertEquals(text, ((SerialClob) rowSet.getObject(2)).getSubString(1, text.length()));
        } finally {
            server.close();
        }
    }

    @Test
    public void knownLengthAboveLimitIsSpoolAndDeletedOnClose() throws Exception {
        byte[] blob = new byte[LobTypes.INLINE_LIMIT + 1];
        blob[0] = 9;
        blob[blob.length - 1] = 8;
        String text = largeText();
        ResultSetServer server = load("select b, c from lob_t where id = 2", blob, text);
        LobRef blobRef = null;
        LobRef clobRef = null;
        try {
            SerialRowSetImpl rowSet = (SerialRowSetImpl) server.fetchRows(10);
            Assert.assertTrue(rowSet.next());
            blobRef = (LobRef) rowSet.getObject(1);
            clobRef = (LobRef) rowSet.getObject(2);
            Assert.assertEquals(Types.BLOB, blobRef.getSqlType());
            Assert.assertEquals(blob.length, blobRef.getLength());
            Assert.assertEquals(Types.CLOB, clobRef.getSqlType());
            Assert.assertEquals(text.length(), clobRef.getLength());
            Assert.assertArrayEquals(blob, readAll(blobRef.getLobId()));
            Assert.assertEquals(text, new String(readAll(clobRef.getLobId()), "UTF-8"));
            Assert.assertTrue(LobStore.exists(blobRef.getLobId()));

            byte[] copied = SerializeUtil.serialize(rowSet);
            SerialRowSetImpl restored = (SerialRowSetImpl) SerializeUtil.deserialize(copied);
            Assert.assertEquals(blobRef.getLobId(), ((LobRef) restored.getObject(1)).getLobId());
        } finally {
            server.close();
        }
        Assert.assertFalse(LobStore.exists(blobRef.getLobId()));
        Assert.assertFalse(LobStore.exists(clobRef.getLobId()));
    }

    @Test
    public void channelCloseDeletesSpoolFiles() throws Exception {
        byte[] blob = new byte[LobTypes.INLINE_LIMIT + 1];
        Arrays.fill(blob, (byte) 3);
        DispatchChannels.set(42);
        ResultSetServer server = load("select b from lob_t where id = 3", blob, "x");
        DispatchChannels.clear();
        SerialRowSetImpl rowSet = (SerialRowSetImpl) server.fetchRows(1);
        Assert.assertTrue(rowSet.next());
        LobRef ref = (LobRef) rowSet.getObject(1);
        Assert.assertTrue(LobStore.exists(ref.getLobId()));
        ResultSetServer.closeChannel(42);
        Assert.assertFalse(LobStore.exists(ref.getLobId()));
    }

    @Test
    public void unknownLengthUsesSpool() throws Exception {
        final byte[] data = new byte[]{5, 6, 7, 8, 9};
        ResultSet resultSet = (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(),
                new Class<?>[]{ResultSet.class},
                new InvocationHandler() {
                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                        if ("getBinaryStream".equals(method.getName())) {
                            return new ByteArrayInputStream(data);
                        }
                        throw new UnsupportedOperationException(method.getName());
                    }
                });
        long resultSetId = 9_000_000L;
        try {
            Object value = new ServerLobMaterializer(resultSetId).materialize(resultSet, 1, Types.LONGVARBINARY);
            LobRef ref = (LobRef) value;
            Assert.assertEquals(data.length, ref.getLength());
            Assert.assertArrayEquals(data, readAll(ref.getLobId()));
        } finally {
            LobStore.deleteByResultSet(resultSetId);
        }
    }

    private static ResultSetServer load(String sql, byte[] blob, String text) throws Exception {
        Class.forName("org.h2.Driver");
        Connection connection = DriverManager.getConnection("jdbc:h2:mem:lob_columns;DB_CLOSE_DELAY=-1", "sa", "");
        Statement statement = connection.createStatement();
        statement.execute("create table if not exists lob_t(id int primary key, b blob, c clob)");
        PreparedStatement insert = connection.prepareStatement("merge into lob_t(id, b, c) key(id) values(?, ?, ?)");
        int id = Integer.parseInt(sql.substring(sql.lastIndexOf("= ") + 2).trim());
        insert.setInt(1, id);
        insert.setBytes(2, blob);
        insert.setString(3, text);
        insert.executeUpdate();
        return new ResultSetServer(statement.executeQuery(sql));
    }

    private static String largeText() {
        char[] chars = new char[LobTypes.INLINE_LIMIT + 1];
        Arrays.fill(chars, 'a');
        chars[0] = '中';
        return new String(chars);
    }

    private static byte[] readAll(long lobId) throws Exception {
        LobStore.Chunk chunk = LobStore.read(lobId, 0, LobTypes.CHUNK_SIZE);
        Assert.assertTrue(chunk.eof);
        return chunk.data;
    }
}
