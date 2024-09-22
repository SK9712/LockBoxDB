package com.lockboxdb.query;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.ddl.SqlDdlNodes;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

public class LockBoxQueryProcessor {

    private SqlParser.Config sqlConfig;

    private SqlParser sqlParser;

    private static LockBoxQueryProcessor locketQueryProcessor = null;

    private LockBoxQueryProcessor() {
        sqlConfig = SqlParser.Config.DEFAULT
                .withParserFactory(SqlParserImpl.FACTORY);
        // Create a SqlParser instance
        sqlParser = SqlParser.create("", SqlParser.configBuilder()
                .setParserFactory(SqlDdlParserImpl.FACTORY)
                .setConformance(SqlConformanceEnum.MYSQL_5)
                .setLex(Lex.MYSQL)
                .build());
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
        SqlDialect dialect = MysqlSqlDialect.DEFAULT; // You can switch this to MysqlSqlDialect.DEFAULT

        return sqlNode.toSqlString(dialect);
    }
}
