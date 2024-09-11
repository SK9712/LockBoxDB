package com.lockboxdb.query;

import com.lockboxdb.client.RocksDBClient;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;
import org.rocksdb.RocksDB;

public class LocketQueryProcessor {

    private SqlNode sqlNode;

    public LocketQueryProcessor(String query) {
        try {
            SqlParser.Config config = SqlParser
                    .configBuilder().build();
            // Create a SqlParser instance
            SqlParser parser = SqlParser.create(query, config);

            // Parse the SQL query
            sqlNode = parser.parseQuery();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void executeQuery() throws Exception {
        RocksDBClient rocksDBClient = RocksDBClient.getInstance();
        if (sqlNode instanceof SqlSelect) {
            SqlSelect sqlSelect = (SqlSelect) sqlNode;

            if(sqlSelect.getSelectList().get(0).toString().equalsIgnoreCase("*")) {
                System.out.println(rocksDBClient.readAll(sqlSelect.getFrom().toString()));
            }
        }
    }
}
