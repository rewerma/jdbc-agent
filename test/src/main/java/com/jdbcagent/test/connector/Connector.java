package com.jdbcagent.test.connector;

import com.jdbcagent.test.common.JavaSerializeUtil;
import com.jdbcagent.test.common.Packet;
import com.jdbcagent.test.common.ResponsePacket;
import com.jdbcagent.test.netty.TestInvoker;

import java.io.Serializable;

public class Connector {
    public static Serializable invoke(String classType, Integer key, String method, Serializable... params) {
        Packet packet = new Packet();
        packet.setClassType(classType);
        packet.setKey(key);
        packet.setMethod(method);
        packet.setParams(params);
        byte[] msg = JavaSerializeUtil.serialize(packet);

        byte[] res = TestInvoker.messageReceive(msg);
        ResponsePacket response = (ResponsePacket) JavaSerializeUtil.deserialize(res);
        if (response != null) {
            return response.getResult();
        } else {
            throw new RuntimeException("!!");
        }
    }

    public static Serializable connInvoke(Integer key, String method, Serializable... params) {
        return invoke("Connection", key, method, params);
    }

    public static Serializable stmtInvoke(Integer key, String method, Serializable... params) {
        return invoke("Statement", key, method, params);
    }

    public static Serializable pstmtInvoke(Integer key, String method, Serializable... params) {
        return invoke("PreparedStatement", key, method, params);
    }

    public static Serializable rsInvoke(Integer key, String method, Serializable... params) {
        return invoke("ResultSet", key, method, params);
    }
}
