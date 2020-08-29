package com.sydml.rpc.serialization.serializer.impl;

import com.caucho.hessian.io.HessianInput;
import com.caucho.hessian.io.HessianOutput;
import com.sydml.rpc.serialization.serializer.ISerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class HessianSerializer implements ISerializer {
    @Override
    public <T> byte[] serialize(T object) {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            HessianOutput hessianOutput = new HessianOutput(outputStream);
            hessianOutput.writeObject(object);
            return outputStream.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T deserialize(byte[] data, Class<T> targetClass) {
        try {
            ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
            HessianInput hessianInput = new HessianInput(inputStream);
            return (T) hessianInput.readObject();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
