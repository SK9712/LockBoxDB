package com.lockboxdb.query;

import com.lockboxdb.client.RocksDBClient;
import com.lockboxdb.utils.QueryUtils;
import com.lockboxdb.wrapper.LockBoxData;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlString;

import java.util.*;
import java.util.stream.Collectors;

public class LockBoxQueryProcessor {

    private SqlParser.Config sqlConfig;

    private SqlParser sqlParser;

    private RocksDBClient rocksDBClient;

    private static LockBoxQueryProcessor locketQueryProcessor = null;

    private LockBoxQueryProcessor() {
        sqlConfig = SqlParser
                .configBuilder().build();
        // Create a SqlParser instance
        sqlParser = SqlParser.create("", sqlConfig);
        rocksDBClient = RocksDBClient.getInstance();
    }

    public static LockBoxQueryProcessor getInstance() {
        if(locketQueryProcessor == null) {
            locketQueryProcessor = new LockBoxQueryProcessor();
        }
        return locketQueryProcessor;
    }

    public Object executeQuery(String sqlString) throws Exception {
        SqlNode sqlNode = sqlParser.parseQuery(sqlString);

        System.out.println(sqlNode.getKind());

        validateSql(sqlNode);

        if (sqlNode instanceof SqlSelect) {
            SqlSelect sqlSelect = (SqlSelect) sqlNode;
            List<Map<String, LockBoxData>> rocksData = rocksDBClient.readAll(sqlSelect.getFrom().toString());

            if(sqlSelect.getSelectList().get(0).toString()
                    .equalsIgnoreCase("*")) {
                if(!sqlSelect.hasWhere())
                    return rocksDBClient.readAll(sqlSelect.getFrom().toString());
                else {
                    SqlNode whereNode = sqlSelect.getWhere();
                    return filterDataByWhereClause(rocksData, whereNode);
                }
            }
            List<Map<String, LockBoxData>> selectedData = new ArrayList<>();
            // Apply WHERE clause logic, if present
            if (sqlSelect.hasWhere()) {
                SqlNode whereNode = sqlSelect.getWhere();
                selectedData = filterDataByWhereClause(rocksData, whereNode);
            }

            // If specific columns are selected (not `*`)
            return selectedData.stream()
                    .map(row -> {
                        Map<String, LockBoxData> filteredRow = new HashMap<>();
                        sqlSelect.getSelectList().forEach(column -> {
                            String columnName = column.toString();
                            if (row.containsKey(columnName)) {
                                filteredRow.put(columnName, row.get(columnName));
                            }
                        });
                        return filteredRow;
                    })
                    .collect(Collectors.toList());
        }
        return null;
    }

    private SqlString validateSql(SqlNode sqlNode) {
        SqlDialect dialect = PostgresqlSqlDialect.DEFAULT; // You can switch this to MysqlSqlDialect.DEFAULT

        return sqlNode.toSqlString(dialect);
    }

    private List<Map<String, LockBoxData>> filterDataByWhereClause(List<Map<String, LockBoxData>> data, SqlNode whereNode) {
        if (whereNode instanceof SqlBasicCall) {
            SqlBasicCall basicCall = (SqlBasicCall) whereNode;
            SqlOperator operator = basicCall.getOperator();

            // Handle equality condition (WHERE column = value)
            if (operator.getName().equalsIgnoreCase("=")) {
                SqlNode leftOperand = basicCall.getOperandList().get(0); // column name
                SqlNode rightOperand = basicCall.getOperandList().get(1); // value

                String columnName = leftOperand.toString();
                String value = rightOperand.toString();

                // Filter the data based on the condition
                return data.stream()
                        .filter(row->row.containsKey(columnName))
                        .filter(row -> QueryUtils.parseSingleQuotedString(value)
                                .equalsIgnoreCase(row.get(columnName).getPayload().toString())) // Compare column value
                        .collect(Collectors.toList());
            } else if (operator.getName().equalsIgnoreCase("AND")) {
                // Handle AND logic: both conditions must be true
                SqlNode leftCondition = basicCall.getOperandList().get(0); // column name
                SqlNode rightCondition = basicCall.getOperandList().get(1); // value

                // Recursively apply filtering on both conditions
                List<Map<String, LockBoxData>> leftFiltered = filterDataByWhereClause(data, leftCondition);
                return filterDataByWhereClause(leftFiltered, rightCondition);

            } else if (operator.getName().equalsIgnoreCase("OR")) {
                // Handle OR logic: either condition must be true
                SqlNode leftCondition = basicCall.getOperandList().get(0); // column name
                SqlNode rightCondition = basicCall.getOperandList().get(1); // value

                // Apply filtering on both conditions and combine results
                List<Map<String, LockBoxData>> leftFiltered = filterDataByWhereClause(data, leftCondition);
                List<Map<String, LockBoxData>> rightFiltered = filterDataByWhereClause(data, rightCondition);

                // Combine the results: OR means we want all rows that match either condition
                Set<Map<String, LockBoxData>> resultSet = new HashSet<>(leftFiltered);
                resultSet.addAll(rightFiltered);
                return new ArrayList<>(resultSet);

            }
            // Add more conditions like `!=`, `>`, `<`, `AND`, `OR`, etc.
        }
        // If WHERE clause not recognized, return original data
        return data;
    }
}
