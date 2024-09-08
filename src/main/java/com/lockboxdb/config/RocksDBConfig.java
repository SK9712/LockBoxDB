package com.lockboxdb.config;

import org.rocksdb.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RocksDBConfig {

    private static List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

    static {
        RocksDB.loadLibrary(); // Loads the native RocksDB library
    }

    public static RocksDB createConnection(String dbName) {
        try (final DBOptions dbOptions = new DBOptions().setCreateIfMissing(true)) {
            // List all column families
            List<byte[]> columnFamilyNames = RocksDB.listColumnFamilies(new Options(), dbName);

            // Create column family descriptors
            List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();

            for (byte[] name : columnFamilyNames) {
                cfDescriptors.add(new ColumnFamilyDescriptor(name));
            }

            if (cfDescriptors.size() == 0) {
                cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
            }

            // Prepare list for column family handles

            // Open the database
            return RocksDB.open(dbOptions, dbName, cfDescriptors, columnFamilyHandles);
        } catch (
                Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public static void closeConnection(RocksDB rocksDB) {
        rocksDB.close();
    }

    public static List<ColumnFamilyHandle> getColumnFamilyHandles() {
        return columnFamilyHandles;
    }
}
