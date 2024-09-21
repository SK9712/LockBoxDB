package com.lockboxdb.query;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlString;

public class LockBoxQueryProcessor {

    private SqlParser.Config sqlConfig;

    private SqlParser sqlParser;

    private static LockBoxQueryProcessor locketQueryProcessor = null;

    private LockBoxQueryProcessor() {
        sqlConfig = SqlParser
                .configBuilder().build();
        // Create a SqlParser instance
        sqlParser = SqlParser.create("", sqlConfig);
    }

    public static LockBoxQueryProcessor getInstance() {
        if(locketQueryProcessor == null) {
            locketQueryProcessor = new LockBoxQueryProcessor();
        }
        return locketQueryProcessor;
    }

    public Object executeQuery(String sqlString) throws Exception {
        SqlNode sqlNode = sqlParser.parseQuery(sqlString);

        validateSql(sqlNode);

        return LockBoxQueryParser.valueOf(sqlNode.getKind().name()).lockBoxParse(sqlNode);
    }

    private SqlString validateSql(SqlNode sqlNode) {
        SqlDialect dialect = PostgresqlSqlDialect.DEFAULT; // You can switch this to MysqlSqlDialect.DEFAULT

        return sqlNode.toSqlString(dialect);
    }
}
