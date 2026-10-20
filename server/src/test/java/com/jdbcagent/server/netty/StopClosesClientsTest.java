package com.jdbcagent.server.netty;

import com.jdbcagent.core.util.TransportSecurity;
import com.jdbcagent.server.config.JdbcAgentConf;
import io.netty.channel.group.ChannelGroup;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketException;
import java.util.concurrent.TimeUnit;

public class StopClosesClientsTest {

    private JdbcAgentNettyServer server;

    @After
    public void tearDown() {
        if (server != null && server.isStart()) {
            server.stop();
        }
    }

    @Test
    public void stopClosesAcceptedClients() throws Exception {
        File store = TestCertificates.keyStore();
        int port = freePort();
        server = JdbcAgentNettyServer.instance();
        if (server.isStart()) {
            server.stop();
        }
        JdbcAgentConf conf = new JdbcAgentConf();
        JdbcAgentConf.JdbcAgent agent = new JdbcAgentConf.JdbcAgent();
        agent.setIp("127.0.0.1");
        agent.setPort(port);
        agent.setCatalog("stop-clients");
        agent.setSslKeyStore(store.getAbsolutePath());
        agent.setSslKeyStorePassword(TestCertificates.PASSWORD);
        agent.setSslKeyStoreType("PKCS12");
        conf.setJdbcAgent(agent);
        server.setJdbcAgentConf(conf);
        server.start();

        SSLContext clientContext = TransportSecurity.clientContext(
                store.getAbsolutePath(), TestCertificates.PASSWORD, "PKCS12");
        SSLSocket first = connect(clientContext, port);
        SSLSocket second = connect(clientContext, port);
        waitUntilTracked(3);

        server.stop();

        assertClosed(first);
        assertClosed(second);
        assertListenClosed(port);
    }

    private static SSLSocket connect(SSLContext clientContext, int port) throws Exception {
        SSLSocket socket = (SSLSocket) clientContext.getSocketFactory().createSocket();
        socket.connect(new InetSocketAddress("127.0.0.1", port), 2000);
        TransportSecurity.restrict(socket);
        socket.setSoTimeout(2000);
        socket.startHandshake();
        return socket;
    }

    private static void waitUntilTracked(int expected) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        int size = trackedChannels();
        while (size < expected) {
            if (System.nanoTime() > deadline) {
                Assert.fail("accepted clients were not tracked, size=" + size);
            }
            Thread.sleep(10);
            size = trackedChannels();
        }
    }

    private static int trackedChannels() throws Exception {
        Field field = JdbcAgentNettyServer.class.getDeclaredField("childGroups");
        field.setAccessible(true);
        ChannelGroup group = (ChannelGroup) field.get(JdbcAgentNettyServer.instance());
        return group.size();
    }

    private static void assertClosed(SSLSocket client) throws IOException {
        try {
            Assert.assertEquals(-1, client.getInputStream().read());
        } catch (SocketException closed) {
            Assert.assertNotNull(closed.getMessage());
        } finally {
            client.close();
        }
    }

    private static void assertListenClosed(int port) throws IOException {
        java.net.Socket late = new java.net.Socket();
        try {
            late.connect(new InetSocketAddress("127.0.0.1", port), 1000);
            Assert.fail("listen socket still accepts connections");
        } catch (IOException expected) {
            Assert.assertNotNull(expected.getMessage());
        } finally {
            late.close();
        }
    }

    private static int freePort() throws IOException {
        ServerSocket socket = new ServerSocket(0);
        try {
            return socket.getLocalPort();
        } finally {
            socket.close();
        }
    }
}
