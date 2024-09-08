package com.lockboxdb.datatypes;

public interface DataType {

    byte[] serialize(Object value);

    Object deserialize(byte[] value);
}
