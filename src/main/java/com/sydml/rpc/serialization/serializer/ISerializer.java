package com.sydml.rpc.serialization.serializer;

public interface ISerializer {
    /**
     * 序列化
     * @param object
     * @param <T>
     * @return
     */
    <T> byte[] serialize(T object);

    /**
     * 反序列化
     * @param data
     * @param targetClass
     * @param <T>
     * @return
     */
    <T> T deserialize(byte[] data, Class<T> targetClass);
}
