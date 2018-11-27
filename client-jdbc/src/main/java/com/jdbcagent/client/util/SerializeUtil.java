package com.jdbcagent.client.util;

import com.jdbcagent.core.protocol.Packet;

/**
 * 序列化方式
 *
 * @author Machengyuan
 * @version 1.0 2018-08-10
 */
public class SerializeUtil {
    /*
     * 序列化方式, 由服务端指定, 默认为java序列化
     */
    public static Packet.SerializeType serializeType = Packet.SerializeType.java;
}
