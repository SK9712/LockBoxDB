package com.lockboxdb.query;

import com.lockboxdb.utils.QueryUtils;
import com.lockboxdb.wrapper.LockBoxData;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;

import java.util.*;
import java.util.stream.Collectors;

public class LockBoxQueryFilter {

    public static List<Map<String, LockBoxData>> filterDataByWhereClause(List<Map<String, LockBoxData>> data, SqlNode whereNode) {
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
        // If WHERE clause not recognized, return original data
        return data;
    }

}
