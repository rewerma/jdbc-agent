package com.jdbcagent.server.netty;

import com.jdbcagent.core.util.TransportSecurity;
import com.jdbcagent.server.config.JdbcAgentConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import java.io.File;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

public class TlsTransportTest {
    private JdbcAgentNettyServer server;

    @After
    public void tearDown() {
        if (server != null && server.isStart()) {
            server.stop();
        }
    }

    @Test
    public void handshakeUsesTlsAndRejectsPlaintext() throws Exception {
        File store = TestCertificates.keyStore();
        int port = freePort();
        server = start(port, store);

        Socket tcp = new Socket();
        tcp.connect(new InetSocketAddress("127.0.0.1", port), 2000);
        SSLSocket tls = (SSLSocket) TransportSecurity.clientContext(
                store.getAbsolutePath(), TestCertificates.PASSWORD, "PKCS12").getSocketFactory()
                .createSocket(tcp, "127.0.0.1", port, true);
        try {
            TransportSecurity.restrict(tls);
            tls.startHandshake();
            SSLSession session = tls.getSession();
            Assert.assertTrue(session.getProtocol().startsWith("TLSv1."));
            Assert.assertFalse(session.getCipherSuite().contains("NULL"));
            Assert.assertFalse(session.getCipherSuite().contains("anon"));
        } finally {
            tls.close();
        }

        Socket plain = new Socket();
        plain.connect(new InetSocketAddress("127.0.0.1", port), 2000);
        plain.setSoTimeout(2000);
        try {
            plain.getOutputStream().write(new byte[]{0, 0, 0, 4, 1, 2, 3, 4});
            plain.getOutputStream().flush();
            InputStream in = plain.getInputStream();
            try {
                int ignored = in.read();
                while (ignored >= 0) {
                    ignored = in.read();
                }
            } catch (SocketException closed) {
                Assert.assertNotNull(closed.getMessage());
            }
        } finally {
            plain.close();
        }
    }

    private static JdbcAgentNettyServer start(int port, File store) {
        JdbcAgentNettyServer server = JdbcAgentNettyServer.instance();
        if (server.isStart()) {
            server.stop();
        }
        JdbcAgentConf conf = new JdbcAgentConf();
        JdbcAgentConf.JdbcAgent agent = new JdbcAgentConf.JdbcAgent();
        agent.setIp("127.0.0.1");
        agent.setPort(port);
        agent.setCatalog("tls");
        agent.setSslKeyStore(store.getAbsolutePath());
        agent.setSslKeyStorePassword(TestCertificates.PASSWORD);
        agent.setSslKeyStoreType("PKCS12");
        conf.setJdbcAgent(agent);
        server.setJdbcAgentConf(conf);
        server.start();
        return server;
    }

    private static int freePort() throws Exception {
        ServerSocket socket = new ServerSocket(0);
        try {
            return socket.getLocalPort();
        } finally {
            socket.close();
        }
    }
}
