package com.lockboxdb.wrapper;

import java.io.Serializable;

public class Locket implements Serializable {

    private String dataType;

    private Object payload;

    public Locket(String dataType, Object payload) {
        this.dataType = dataType;
        this.payload = payload;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public Object getPayload() {
        return payload;
    }

    public void setPayload(Object payload) {
        this.payload = payload;
    }
}
