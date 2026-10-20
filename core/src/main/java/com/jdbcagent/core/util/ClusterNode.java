package com.jdbcagent.core.util;

import java.nio.charset.StandardCharsets;

/**
 * 集群成员。二进制负载按连接数，weight 越大分到的连接越多。
 */
public final class ClusterNode {
    private String address;
    private int weight = 1;
    private int activeConnections;

    public ClusterNode() {
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    public int getActiveConnections() {
        return activeConnections;
    }

    public void setActiveConnections(int activeConnections) {
        this.activeConnections = activeConnections;
    }

    public static byte[] toBytes(ClusterNode node) {
        String json = "{\"address\":" + quote(node.getAddress())
                + ",\"weight\":" + Math.max(1, node.getWeight())
                + ",\"activeConnections\":" + Math.max(0, node.getActiveConnections()) + "}";
        return json.getBytes(StandardCharsets.UTF_8);
    }

    public static ClusterNode parse(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        String json = new String(bytes, StandardCharsets.UTF_8);
        String address = stringField(json, "address");
        if (address == null || address.length() == 0) {
            return null;
        }
        ClusterNode node = new ClusterNode();
        node.setAddress(address);
        Integer weight = intField(json, "weight");
        node.setWeight(weight == null || weight.intValue() < 1 ? 1 : weight.intValue());
        Integer active = intField(json, "activeConnections");
        node.setActiveConnections(active == null || active.intValue() < 0 ? 0 : active.intValue());
        return node;
    }

    private static String quote(String value) {
        if (value == null) {
            return "null";
        }
        StringBuilder builder = new StringBuilder(value.length() + 2);
        builder.append('"');
        for (int i = 0; i < value.length(); i++) {
            char current = value.charAt(i);
            if (current == '"' || current == '\\') {
                builder.append('\\').append(current);
            } else {
                builder.append(current);
            }
        }
        builder.append('"');
        return builder.toString();
    }

    private static String stringField(String json, String name) {
        int valueAt = valueStart(json, name);
        if (valueAt < 0 || json.startsWith("null", valueAt) || json.charAt(valueAt) != '"') {
            return null;
        }
        return readString(json, valueAt + 1);
    }

    private static Integer intField(String json, String name) {
        int valueAt = valueStart(json, name);
        if (valueAt < 0) {
            return null;
        }
        int end = valueAt;
        if (end < json.length() && (json.charAt(end) == '-' || json.charAt(end) == '+')) {
            end++;
        }
        int digits = end;
        while (end < json.length() && Character.isDigit(json.charAt(end))) {
            end++;
        }
        if (digits == end) {
            return null;
        }
        try {
            return Integer.valueOf(json.substring(valueAt, end));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static int valueStart(String json, String name) {
        String key = "\"" + name + "\"";
        int from = 0;
        while (from < json.length()) {
            int keyAt = json.indexOf(key, from);
            if (keyAt < 0) {
                return -1;
            }
            int index = keyAt + key.length();
            while (index < json.length() && Character.isWhitespace(json.charAt(index))) {
                index++;
            }
            if (index < json.length() && json.charAt(index) == ':') {
                index++;
                while (index < json.length() && Character.isWhitespace(json.charAt(index))) {
                    index++;
                }
                if (index < json.length()) {
                    return index;
                }
            }
            from = keyAt + key.length();
        }
        return -1;
    }

    private static String readString(String json, int start) {
        StringBuilder value = new StringBuilder();
        for (int index = start; index < json.length(); index++) {
            char current = json.charAt(index);
            if (current == '\\') {
                if (index + 1 >= json.length()) {
                    break;
                }
                value.append(json.charAt(index + 1));
                index++;
            } else if (current == '"') {
                return value.toString();
            } else {
                value.append(current);
            }
        }
        return value.toString();
    }
}
