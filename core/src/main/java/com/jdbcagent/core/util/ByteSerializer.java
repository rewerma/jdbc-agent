package com.jdbcagent.core.util;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

import java.io.UnsupportedEncodingException;

/**
 * JDBC-Agent zookeeper 的序列化
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class ByteSerializer implements ZkSerializer {

    public Object deserialize(final byte[] bytes) throws ZkMarshallingError {
        return bytes;
    }

    public byte[] serialize(final Object data) throws ZkMarshallingError {
        try {
            if (data instanceof byte[]) {
                return (byte[]) data;
            } else {
                return ((String) data).getBytes("utf-8");
            }
        } catch (final UnsupportedEncodingException e) {
            throw new ZkMarshallingError(e);
        }
    }

}
