package com.lockboxdb.query;

import com.lockboxdb.client.RocksDBClient;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;

public class LocketQueryProcessor {

    private SqlParser.Config sqlConfig;

    private SqlParser sqlParser;

    private RocksDBClient rocksDBClient;

    private static LocketQueryProcessor locketQueryProcessor = null;

    private LocketQueryProcessor() {
        sqlConfig = SqlParser
                .configBuilder().build();
        // Create a SqlParser instance
        sqlParser = SqlParser.create("", sqlConfig);
        rocksDBClient = RocksDBClient.getInstance();
    }

    public static LocketQueryProcessor getInstance() {
        if(locketQueryProcessor == null) {
            locketQueryProcessor = new LocketQueryProcessor();
        }
        return locketQueryProcessor;
    }

    public Object executeQuery(String sqlString) throws Exception {
        SqlNode sqlNode = sqlParser.parseQuery(sqlString);

        if (sqlNode instanceof SqlSelect) {
            SqlSelect sqlSelect = (SqlSelect) sqlNode;

            if(sqlSelect.getSelectList().get(0).toString().equalsIgnoreCase("*")) {
                return rocksDBClient.readAll(sqlSelect.getFrom().toString());
            }
        }
        return null;
    }
}
