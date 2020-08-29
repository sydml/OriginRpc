package com.sydml.rpc.serialization.common;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class FDateJsonDeserializer extends JsonDeserializer<LocalDateTime> {
    static final String PATTERN = "yyyy-MM-dd HH:mm:ss";
    static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(PATTERN);


    @Override
    public LocalDateTime deserialize(JsonParser gen, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        String date = gen.getText();

        if (StringUtils.isEmpty(date)) {
            return null;
        }

        if (StringUtils.isNumeric(date)) {
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.valueOf(date)), ZoneId.systemDefault());
        }
        try {
            return LocalDateTime.parse(date, dateTimeFormatter);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

}
