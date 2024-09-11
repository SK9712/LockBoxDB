//package com.lockboxdb;
//
//import org.apache.calcite.sql.SqlDialect;
//import org.apache.calcite.sql.SqlNode;
//import org.apache.calcite.sql.parser.SqlParser;
//
//public class DataManagementService {
//    public void get(String query) {
//        // Create a SqlParser.Config object with default settings
//        SqlParser.Config config = SqlParser.configBuilder().build();
//
//        // Create a SqlParser instance
//        SqlParser parser = SqlParser.create("SELECT * FROM my_table", config);
//
//        // Parse the SQL query
//        SqlNode sqlNode = parser.parseQuery();
//
//        // Print the parsed SQL node
//        System.out.println("Parsed SQL Node: " + sqlNode.toString());
//
////        SqlParser parser = SqlParser.create("SELECT * FROM my_table", config);
////        SqlNode sqlNode = parser.parseQuery();
//    }
//}
