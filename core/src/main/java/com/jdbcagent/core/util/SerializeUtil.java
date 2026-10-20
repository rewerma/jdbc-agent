package com.jdbcagent.core.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.MapReferenceResolver;
import com.esotericsoftware.kryo.util.Pool;

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

        Pool<Kryo> pool = null;
        Kryo kryo = null;
        ByteArrayOutputStream baos = null;
        Output output = null;
        try {
            pool = KryoPoolFactory.getKryoPoolInstance();
            kryo = pool.obtain();
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
                pool.free(kryo);
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
        Pool<Kryo> pool = null;
        Kryo kryo = null;
        Input input = null;
        try {
            pool = KryoPoolFactory.getKryoPoolInstance();
            kryo = pool.obtain();
            input = new Input(bytes);
            return kryo.readClassAndObject(input);
        } finally {
            if (input != null) {
                input.close();
            }
            if (kryo != null) {
                pool.free(kryo);
            }
        }
    }

    /**
     * Kryo 对象池
     */
    private static class KryoPoolFactory {
        private static volatile KryoPoolFactory poolFactory = null;

        private Pool<Kryo> pool;

        private KryoPoolFactory() {
            pool = new Pool<Kryo>(true, false, 32) {
                @Override
                protected Kryo create() {
                    Kryo kryo = new Kryo(new AllowedClassResolver(), new MapReferenceResolver());
                    kryo.setReferences(false);
                    kryo.setRegistrationRequired(true);
                    KryoClassRegistry.register(kryo);
                    return kryo;
                }
            };
        }

        static Pool<Kryo> getKryoPoolInstance() {
            if (poolFactory == null) {
                synchronized (KryoPoolFactory.class) {
                    if (poolFactory == null) {
                        poolFactory = new KryoPoolFactory();
                    }
                }
            }
            return poolFactory.getPool();
        }

        private Pool<Kryo> getPool() {
            return pool;
        }
    }
}
