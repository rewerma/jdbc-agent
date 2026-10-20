package com.jdbcagent.server.running;

import org.junit.Assert;
import org.junit.Test;

public class ServerRunningDataCodecTest {

    @Test
    public void keepsAddressFieldReadableByClients() throws Exception {
        ServerRunningData data = new ServerRunningData();
        data.setCatalog("sales");
        data.setAddress("10.0.0.8:10101");
        data.setActive(true);

        byte[] bytes = ServerRunningDataCodec.toBytes(data);
        ServerRunningData parsed = ServerRunningDataCodec.parse(bytes);
        Assert.assertEquals("sales", parsed.getCatalog());
        Assert.assertEquals("10.0.0.8:10101", parsed.getAddress());
        Assert.assertTrue(parsed.isActive());
        String json = new String(bytes, "UTF-8");
        Assert.assertTrue(json.contains("\"address\":\"10.0.0.8:10101\""));
    }
}
