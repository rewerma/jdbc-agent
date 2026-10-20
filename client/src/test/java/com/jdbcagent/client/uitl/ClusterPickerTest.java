package com.jdbcagent.client.uitl;

import com.jdbcagent.core.util.ClusterNode;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class ClusterPickerTest {

    @Test
    public void prefersLowerConnectionsPerWeight() {
        ClusterNode heavy = node("10.0.0.1:10101", 1, 10);
        ClusterNode light = node("10.0.0.2:10101", 1, 2);
        ClusterNode weighted = node("10.0.0.3:10101", 4, 4);

        Assert.assertEquals("10.0.0.3:10101",
                ClusterPicker.pick(Arrays.asList(heavy, light, weighted), Collections.<String>emptySet()).getAddress());
    }

    @Test
    public void skipsExcludedMembers() {
        ClusterNode first = node("10.0.0.1:10101", 1, 1);
        ClusterNode second = node("10.0.0.2:10101", 1, 9);
        Set<String> excluded = new HashSet<String>();
        excluded.add("10.0.0.1:10101");

        Assert.assertEquals("10.0.0.2:10101",
                ClusterPicker.pick(Arrays.asList(first, second), excluded).getAddress());
        Assert.assertNull(ClusterPicker.pick(Arrays.asList(first), excluded));
    }

    @Test
    public void rotatesEqualLoad() {
        ClusterNode left = node("10.0.0.1:10101", 1, 3);
        ClusterNode right = node("10.0.0.2:10101", 2, 6);
        Set<String> seen = new HashSet<String>();
        for (int i = 0; i < 8; i++) {
            seen.add(ClusterPicker.pick(Arrays.asList(left, right), Collections.<String>emptySet()).getAddress());
        }
        Assert.assertEquals(new HashSet<String>(Arrays.asList("10.0.0.1:10101", "10.0.0.2:10101")), seen);
    }

    private static ClusterNode node(String address, int weight, int active) {
        ClusterNode node = new ClusterNode();
        node.setAddress(address);
        node.setWeight(weight);
        node.setActiveConnections(active);
        return node;
    }
}
