package com.lockboxdb.query;

import com.lockboxdb.client.RocksDBClient;
import com.lockboxdb.wrapper.LockBoxData;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public enum LockBoxQueryParser {
    SELECT {
        public Object lockBoxParse(SqlNode sqlNode) throws Exception {
            SqlSelect sqlSelect = (SqlSelect) sqlNode;
            List<Map<String, LockBoxData>> rocksData = RocksDBClient.getInstance()
                    .readAll(sqlSelect.getFrom().toString());

            if (sqlSelect.getSelectList().get(0).toString()
                    .equalsIgnoreCase("*")) {
                if (!sqlSelect.hasWhere())
                    return RocksDBClient.getInstance().readAll(sqlSelect.getFrom().toString());
                else {
                    SqlNode whereNode = sqlSelect.getWhere();
                    return LockBoxQueryFilter.filterDataByWhereClause(rocksData, whereNode);
                }
            }
            List<Map<String, LockBoxData>> selectedData = new ArrayList<>();
            // Apply WHERE clause logic, if present
            if (sqlSelect.hasWhere()) {
                SqlNode whereNode = sqlSelect.getWhere();
                selectedData = LockBoxQueryFilter
                        .filterDataByWhereClause(rocksData, whereNode);
            }

            // If specific columns are selected (not `*`)
            return selectedData.stream()
                    .map(row -> filterData(sqlSelect, row))
                    .collect(Collectors.toList());
        }

        private Map<String, LockBoxData> filterData(SqlSelect sqlSelect, Map<String, LockBoxData> row) {
            Map<String, LockBoxData> filteredRow = new HashMap<>();
            sqlSelect.getSelectList().stream().map(column -> column.toString())
                    .filter(column -> row.containsKey(column)).forEach(columnName ->
                            filteredRow.put(columnName, row.get(columnName)));
            return filteredRow;
        }
    };

    public abstract Object lockBoxParse(SqlNode sqlNode) throws Exception;
}
