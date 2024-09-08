package com.lockboxdb.struct;

import org.rocksdb.ColumnFamilyHandle;

import java.io.Serializable;

public class Relation implements Serializable {
    private ColumnFamilyHandle columnFamilyHandle;

    public Relation(ColumnFamilyHandle columnFamilyHandle) {
        this.columnFamilyHandle = columnFamilyHandle;
    }

    public ColumnFamilyHandle getColumnFamilyHandle() {
        return columnFamilyHandle;
    }

    public void setColumnFamilyHandle(ColumnFamilyHandle columnFamilyHandle) {
        this.columnFamilyHandle = columnFamilyHandle;
    }
}
