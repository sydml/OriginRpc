package com.sydml.rpc.serialization.serializer.impl;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.sydml.rpc.serialization.serializer.ISerializer;
import com.sydml.rpc.serialization.common.FDateJsonDeserializer;
import com.sydml.rpc.serialization.common.FDateJsonSerializer;

import java.time.LocalDateTime;

public class JSONSerializer implements ISerializer {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    static {
        objectMapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        objectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        objectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        SimpleModule module = new SimpleModule("DateTimeModule", Version.unknownVersion());
        module.addSerializer(LocalDateTime.class, new FDateJsonSerializer());
        module.addDeserializer(LocalDateTime.class, new FDateJsonDeserializer());
        objectMapper.registerModule(module);
    }

    public static ObjectMapper getObjectMapperInstance(){
        return objectMapper;
    }
    @Override
    public <T> byte[] serialize(T object) {
        try {
            return objectMapper.writeValueAsBytes(object);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T> T deserialize(byte[] data, Class<T> targetClass) {
        String json = new String(data);
        try {
            return (T) objectMapper.readValue(json, targetClass);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
