package com.lockboxdb.query;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;

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

    public void executeQuery() {
        if (sqlNode instanceof SqlSelect) {
            SqlSelect sqlSelect = (SqlSelect) sqlNode;
            System.out.println("Parsed SQL Node: " +
                    sqlSelect.getFrom().toString());
        }
    }
}
