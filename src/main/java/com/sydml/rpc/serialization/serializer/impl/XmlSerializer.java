package com.sydml.rpc.serialization.serializer.impl;

import com.sydml.rpc.serialization.serializer.ISerializer;

import java.beans.XMLDecoder;
import java.beans.XMLEncoder;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class XmlSerializer implements ISerializer {
    @Override
    public <T> byte[] serialize(T object) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try(XMLEncoder xmlEncoder = new XMLEncoder(byteArrayOutputStream, "utf-8", true, 0)){
            xmlEncoder.writeObject(object);
        }
        return byteArrayOutputStream.toByteArray();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T deserialize(byte[] data, Class<T> targetClass) {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
        try (XMLDecoder xmlDecoder = new XMLDecoder(byteArrayInputStream)) {
            return (T) xmlDecoder.readObject();
        }
    }
}
