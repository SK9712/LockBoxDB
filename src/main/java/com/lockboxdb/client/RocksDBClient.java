package com.lockboxdb.client;

import com.lockboxdb.config.RocksDBConfig;

import com.lockboxdb.utils.DBUtils;
import com.lockboxdb.wrapper.Locket;
import org.rocksdb.*;

import java.nio.charset.StandardCharsets;

public class RocksDBClient {

    private static RocksDBClient rocksDBClient = null;

    private RocksDB rocksDB;

    private ReadOptions readOptions;

    private WriteOptions writeOptions;

    private RocksDBClient() {
    }

    public static RocksDBClient getInstance() {
        if (rocksDBClient == null) {
            rocksDBClient = new RocksDBClient();
            rocksDBClient.setRocksDB(RocksDBConfig.createConnection("testdb"));
            rocksDBClient.setWriteOptions(new WriteOptions());
            rocksDBClient.setReadOptions(new ReadOptions());
        }

        return rocksDBClient;
    }

    public void write(String key, Object data, String type)
            throws Exception {
        switch (type.toLowerCase()) {
            case "string":
                rocksDB.put(writeOptions,
                        key.getBytes(StandardCharsets.UTF_8),
                        DBUtils.serialize(new Locket("string", data)));
                break;
        }
    }

    public void write(String tableName, String key,
                      Object data, String type)
            throws Exception {
        switch (type.toLowerCase()) {
            case "string":
                int handleId = ((Integer) DBUtils.deserialize(
                        rocksDB.get(tableName.getBytes(StandardCharsets.UTF_8)))).intValue();

                ColumnFamilyHandle tableHandle = RocksDBConfig.getColumnFamilyHandles().get(handleId);
                rocksDB.put(tableHandle,
                        key.getBytes(StandardCharsets.UTF_8),
                        DBUtils.serialize(new Locket("string", data)));
                break;
        }
    }

    public Object read(String key, String type)
            throws Exception {
        switch (type.toLowerCase()) {
            case "string":
                return ((Locket) DBUtils.deserialize(rocksDB.get(readOptions,
                        key.getBytes(StandardCharsets.UTF_8)))).getPayload();
            default:
                throw new RuntimeException("Invalid DataType");
        }
    }

    public Object read(String tableName, String key, String type)
            throws Exception {
        switch (type.toLowerCase()) {
            case "string":
                int handleId = ((Integer) DBUtils.deserialize(
                        rocksDB.get(tableName.getBytes(StandardCharsets.UTF_8)))).intValue();

                ColumnFamilyHandle tableHandle = RocksDBConfig.getColumnFamilyHandles().get(handleId);

                return ((Locket) DBUtils.deserialize(rocksDB.get(tableHandle,
                        key.getBytes(StandardCharsets.UTF_8)))).getPayload();
            default:
                throw new RuntimeException("Invalid DataType");
        }
    }

    public void createTable(String tableName)
            throws Exception {
        ColumnFamilyHandle tableHandle = rocksDB.createColumnFamily(new
                ColumnFamilyDescriptor(tableName.getBytes(StandardCharsets.UTF_8)));
        rocksDB.put(tableName.getBytes(StandardCharsets.UTF_8),
                DBUtils.serialize(new Locket("number",
                        new Integer(tableHandle.getID()))));
        RocksDBConfig.getColumnFamilyHandles().add(tableHandle);
    }

    private void setRocksDB(RocksDB rocksDB) {
        this.rocksDB = rocksDB;
    }

    private void setReadOptions(ReadOptions readOptions) {
        this.readOptions = readOptions;
    }

    private void setWriteOptions(WriteOptions writeOptions) {
        this.writeOptions = writeOptions;
    }
}
