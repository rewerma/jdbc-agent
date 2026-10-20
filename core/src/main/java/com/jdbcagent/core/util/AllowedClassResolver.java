package com.jdbcagent.core.util;

import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.util.DefaultClassResolver;
import com.esotericsoftware.kryo.util.IntMap;
import com.esotericsoftware.kryo.util.ObjectMap;

/**
 * 按类名读取时，先核对白名单，再加载类。未登记的类名不会进入 Class.forName。
 */
public class AllowedClassResolver extends DefaultClassResolver {

    @Override
    protected Registration readName(Input input) {
        int nameId = input.readVarInt(true);
        if (nameIdToClass == null) {
            nameIdToClass = new IntMap<Class>();
        }
        Class type = nameIdToClass.get(nameId);
        if (type == null) {
            String className = input.readString();
            if (!KryoClassRegistry.isAllowed(className)) {
                throw new KryoException("Class is not allowed: " + className);
            }
            type = getTypeByName(className);
            if (type == null) {
                try {
                    type = Class.forName(className, false, kryo.getClassLoader());
                } catch (ClassNotFoundException ex) {
                    try {
                        type = Class.forName(className);
                    } catch (ClassNotFoundException nested) {
                        throw new KryoException("Unable to find class: " + className, ex);
                    }
                }
                if (nameToClass == null) {
                    nameToClass = new ObjectMap<String, Class>();
                }
                nameToClass.put(className, type);
            }
            nameIdToClass.put(nameId, type);
        }
        return kryo.getRegistration(type);
    }
}
