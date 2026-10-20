package com.jdbcagent.server.running;

import java.nio.charset.StandardCharsets;

/**
 * 主备节点数据的 JSON。字段名保持 catalog、address、active，客户端仍按 address 解析。
 */
final class ServerRunningDataCodec {
    private ServerRunningDataCodec() {
    }

    static byte[] toBytes(ServerRunningData data) {
        String json = "{\"catalog\":" + quote(data.getCatalog())
                + ",\"address\":" + quote(data.getAddress())
                + ",\"active\":" + data.isActive() + "}";
        return json.getBytes(StandardCharsets.UTF_8);
    }

    static ServerRunningData parse(byte[] bytes) {
        String json = new String(bytes, StandardCharsets.UTF_8);
        ServerRunningData data = new ServerRunningData();
        data.setCatalog(stringField(json, "catalog"));
        data.setAddress(stringField(json, "address"));
        Boolean active = booleanField(json, "active");
        if (active != null) {
            data.setActive(active.booleanValue());
        }
        return data;
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
        if (valueAt < 0 || json.startsWith("null", valueAt)) {
            return null;
        }
        if (json.charAt(valueAt) != '"') {
            return null;
        }
        return readString(json, valueAt + 1);
    }

    private static Boolean booleanField(String json, String name) {
        int valueAt = valueStart(json, name);
        if (valueAt < 0) {
            return null;
        }
        if (json.startsWith("true", valueAt)) {
            return Boolean.TRUE;
        }
        if (json.startsWith("false", valueAt)) {
            return Boolean.FALSE;
        }
        return null;
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
