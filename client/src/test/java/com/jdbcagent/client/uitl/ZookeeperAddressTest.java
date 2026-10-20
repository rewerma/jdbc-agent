package com.jdbcagent.client.uitl;

import com.jdbcagent.client.uitl.ZookeeperUtil.AddressHub;
import com.jdbcagent.client.uitl.ZookeeperUtil.AddressListener;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ZookeeperAddressTest {

    @Test
    public void readsAddressField() throws Exception {
        Assert.assertEquals("10.0.0.2:9090", ZookeeperUtil.readAddress(
                "{\"active\":true,\"address\":\"10.0.0.2:9090\",\"catalog\":\"demo\"}"));
        Assert.assertEquals("10.0.0.2:9090", ZookeeperUtil.readAddress(
                "{\"address\" : \"10.0.0.2:9090\"}"));
        Assert.assertEquals("10.0.0.1:1", ZookeeperUtil.readAddress(
                "{\"catalog\":\"address\",\"address\":\"10.0.0.1:1\"}"));
        Assert.assertEquals("10.0.0.8:10101", ZookeeperUtil.readAddress(
                "{\"catalog\":\"sales\",\"address\":\"10.0.0.8:10101\",\"active\":true}"));
    }

    @Test
    public void missingAddressFails() {
        try {
            ZookeeperUtil.readAddress("{\"catalog\":\"demo\"}");
            Assert.fail("missing address should fail");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("address"));
        }
    }

    @Test
    public void hubNotifiesOnlyWhenAddressChanges() {
        AddressHub hub = new AddressHub();
        final AtomicInteger calls = new AtomicInteger();
        final AtomicReference<String> latest = new AtomicReference<String>();
        hub.add(new AddressListener() {
            @Override
            public void onAddress(String address) {
                calls.incrementAndGet();
                latest.set(address);
            }
        });

        hub.publish("10.0.0.1:10101");
        hub.publish("10.0.0.1:10101");
        hub.publishInitial("10.0.0.9:1");
        Assert.assertEquals(1, calls.get());
        Assert.assertEquals("10.0.0.1:10101", latest.get());

        hub.clear();
        hub.publish("10.0.0.2:10101");
        Assert.assertEquals(2, calls.get());
        Assert.assertEquals("10.0.0.2:10101", hub.current());
    }
}
