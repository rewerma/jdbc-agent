package com.jdbcagent.core.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;

/**
 * JDBC-Agent kryo serialize util
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class SerializeUtil {
    /**
     * 序列化
     *
     * @param obj 可序列化对象
     * @return 二进制数据
     */
    public static byte[] serialize(Object obj) {
        if (obj == null)
            return null;

        KryoPool pool = null;
        Kryo kryo = null;
        ByteArrayOutputStream baos = null;
        Output output = null;
        try {
            pool = KryoPoolFactory.getKryoPoolInstance();
            kryo = pool.borrow();
            baos = new ByteArrayOutputStream();
            output = new Output(baos);
            kryo.writeClassAndObject(output, obj);
            output.flush();
            byte[] b = baos.toByteArray();
            baos.flush();
            return b;
        } catch (IOException e) {
            return null;
        } finally {
            if (output != null) {
                output.close();
            }
            if (baos != null) {
                try {
                    baos.close();
                } catch (IOException e) {
                    // ignore
                }
            }
            if (kryo != null) {
                pool.release(kryo);
            }
        }
    }

    /**
     * 反序列化
     *
     * @param bytes 二进制数据
     * @return 反序列化对象
     */
    public static Object deserialize(byte[] bytes) {
        if (bytes == null)
            return null;
        KryoPool pool = null;
        Kryo kryo = null;
        Input input = null;
        try {
            pool = KryoPoolFactory.getKryoPoolInstance();
            kryo = pool.borrow();
            input = new Input(bytes);
            return kryo.readClassAndObject(input);
        } finally {
            if (input != null) {
                input.close();
            }
            if (kryo != null) {
                pool.release(kryo);
            }
        }
    }

    /**
     * Kryo 对象池
     */
    private static class KryoPoolFactory {
        private static volatile KryoPoolFactory poolFactory = null;

        private KryoPool pool;

        private KryoPoolFactory() {
            pool = new KryoPool.Builder(new KryoFactory() {
                public Kryo create() {
                    Kryo kryo = new Kryo();
                    kryo.setReferences(false);
                    // kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
                    return kryo;
                }
            }).build();
        }

        static KryoPool getKryoPoolInstance() {
            if (poolFactory == null) {
                synchronized (KryoPoolFactory.class) {
                    if (poolFactory == null) {
                        poolFactory = new KryoPoolFactory();
                    }
                }
            }
            return poolFactory.getPool();
        }

        private KryoPool getPool() {
            return pool;
        }
    }
}
