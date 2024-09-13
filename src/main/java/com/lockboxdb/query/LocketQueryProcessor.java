package com.lockboxdb.query;

import com.lockboxdb.client.RocksDBClient;
import com.lockboxdb.wrapper.Locket;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
            } else {
                List<Map<String, Locket>> rocksData = rocksDBClient.readAll(sqlSelect.getFrom().toString());
                List<Map<String, Locket>> newData =
                rocksData.stream().map(data->{
                    Map<String, Locket> tempData = new HashMap<>();
                    sqlSelect.getSelectList().stream().forEach(column->tempData.put(column.toString(), data.get(column.toString())));
                    return tempData;
                }).collect(Collectors.toList());
            }
        }
        return null;
    }
}
