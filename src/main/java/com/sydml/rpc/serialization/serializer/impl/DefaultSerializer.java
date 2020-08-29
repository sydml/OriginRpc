package com.sydml.rpc.serialization.serializer.impl;

import com.sydml.rpc.serialization.serializer.ISerializer;

import java.io.*;

public class DefaultSerializer implements ISerializer {
    @Override
    public <T> byte[] serialize(T object) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try(ObjectOutputStream outputStream = new ObjectOutputStream(byteArrayOutputStream)) {
            outputStream.writeObject(object);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return byteArrayOutputStream.toByteArray();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T deserialize(byte[] data, Class<T> targetClass) {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
        try(ObjectInputStream inputStream = new ObjectInputStream(byteArrayInputStream);) {
            return (T) inputStream.readObject();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
