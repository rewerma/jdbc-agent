package com.jdbcagent.client.nio;

import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;

public class NioFailoverAddressTest {

    @Test
    public void directClientHasNoNextAddress() {
        JdbcAgentNioClient client = new JdbcAgentNioClient(new InetSocketAddress("10.0.0.1", 10101), 1000);
        Assert.assertNull(client.getNextAddress());
    }

    @Test
    public void nextAddressReplacesCurrentTarget() {
        final JdbcAgentNioClient client = new JdbcAgentNioClient(new InetSocketAddress("10.0.0.1", 10101), 1000);
        client.setAddressResolver(new JdbcAgentNioClient.AddressResolver() {
            @Override
            public String resolve() {
                return "10.0.0.2:9090";
            }
        });

        Assert.assertEquals(new InetSocketAddress("10.0.0.2", 9090), client.getNextAddress());
        Assert.assertEquals(new InetSocketAddress("10.0.0.2", 9090), client.getAddress());
    }
}
