package com.sydml.rpc.serialization.engine;

import com.sydml.rpc.serialization.serializer.ISerializer;
import com.sydml.rpc.serialization.serializer.impl.HessianSerializer;
import com.sydml.rpc.serialization.serializer.impl.JSONSerializer;
import com.sydml.rpc.serialization.common.SerializeType;
import com.sydml.rpc.serialization.serializer.impl.DefaultSerializer;
import com.sydml.rpc.serialization.serializer.impl.XmlSerializer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SerializerEngine {
    public static final Map<SerializeType, ISerializer> serializerMap = new ConcurrentHashMap<>();

    static {
        serializerMap.put(SerializeType.DefaultJavaSerializer, new DefaultSerializer());
        serializerMap.put(SerializeType.HessianSerializer, new HessianSerializer());
        serializerMap.put(SerializeType.JSONSerializer, new JSONSerializer());
        serializerMap.put(SerializeType.XmlSerializer, new XmlSerializer());
    }


    public static <T> byte[] serialize(T obj, String serializeType) {
        SerializeType serialize = SerializeType.queryByType(serializeType);
        if (serialize == null) {
            throw new RuntimeException("serialize is null");
        }

        ISerializer serializer = serializerMap.get(serialize);
        if (serializer == null) {
            throw new RuntimeException("serialize error");
        }

        try {
            return serializer.serialize(obj);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public static <T> T deserialize(byte[] data, Class<T> clazz, String serializeType) {

        SerializeType serialize = SerializeType.queryByType(serializeType);
        if (serialize == null) {
            throw new RuntimeException("serialize is null");
        }
        ISerializer serializer = serializerMap.get(serialize);
        if (serializer == null) {
            throw new RuntimeException("serialize error");
        }

        try {
            return serializer.deserialize(data, clazz);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
