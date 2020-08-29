package com.sydml.rpc.zookeeper.demo;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.sydml.rpc.serialization.common.FDateJsonDeserializer;
import com.sydml.rpc.serialization.common.FDateJsonSerializer;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

import java.time.LocalDateTime;

public class JSONSerializer implements ZkSerializer {
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
    public byte[] serialize(Object object) throws ZkMarshallingError{
        try {
            return objectMapper.writeValueAsBytes(object);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
        String json = new String(bytes);
        try {
            return  objectMapper.readValue(json, ServerData.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


}
