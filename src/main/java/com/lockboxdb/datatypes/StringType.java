package com.lockboxdb.datatypes;

import java.nio.charset.StandardCharsets;

public class StringType implements DataType {

    @Override
    public byte[] serialize(Object value) {
        return value.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Object deserialize(byte[] value) {
        return new String(value);
    }
}