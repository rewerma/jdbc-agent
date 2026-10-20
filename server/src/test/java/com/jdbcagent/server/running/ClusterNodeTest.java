package com.jdbcagent.server.running;

import com.jdbcagent.core.util.ClusterNode;
import com.jdbcagent.server.config.JdbcAgentConf;
import org.junit.Assert;
import org.junit.Test;

public class ClusterNodeTest {

    @Test
    public void roundTripsAddressWeightAndConnections() {
        ClusterNode node = new ClusterNode();
        node.setAddress("10.0.0.8:10101");
        node.setWeight(3);
        node.setActiveConnections(12);

        ClusterNode parsed = ClusterNode.parse(ClusterNode.toBytes(node));
        Assert.assertEquals("10.0.0.8:10101", parsed.getAddress());
        Assert.assertEquals(3, parsed.getWeight());
        Assert.assertEquals(12, parsed.getActiveConnections());
    }

    @Test
    public void rejectsMissingAddressAndNormalizesWeight() {
        ClusterNode node = new ClusterNode();
        node.setAddress("10.0.0.8:10101");
        node.setWeight(0);
        node.setActiveConnections(-4);

        ClusterNode parsed = ClusterNode.parse(ClusterNode.toBytes(node));
        Assert.assertEquals(1, parsed.getWeight());
        Assert.assertEquals(0, parsed.getActiveConnections());
        Assert.assertNull(ClusterNode.parse("{\"weight\":1}".getBytes()));
    }

    @Test
    public void serverIdReplacesPortSeparator() {
        Assert.assertEquals("10.0.0.8_10101", ServerClusterRegistry.serverId("10.0.0.8:10101"));
    }

    @Test
    public void modeDefaultsToHa() {
        JdbcAgentConf.JdbcAgent agent = new JdbcAgentConf.JdbcAgent();
        Assert.assertFalse(agent.isCluster());
        agent.setMode("cluster");
        Assert.assertTrue(agent.isCluster());
        agent.setWeight(0);
        Assert.assertEquals(1, agent.getWeight());
    }
}
