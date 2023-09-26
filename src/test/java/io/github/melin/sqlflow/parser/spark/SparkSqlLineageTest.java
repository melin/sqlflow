package io.github.melin.sqlflow.parser.spark;

import io.github.melin.sqlflow.analyzer.Analysis;
import io.github.melin.sqlflow.analyzer.StatementAnalyzer;
import io.github.melin.sqlflow.parser.AbstractSqlLineageTest;
import io.github.melin.sqlflow.parser.SqlParser;
import io.github.melin.sqlflow.tree.statement.Statement;
import io.github.melin.sqlflow.util.JsonUtils;
import org.junit.Test;

import java.util.Optional;

import static java.util.Collections.emptyMap;

/**
 * huaixin 2021/12/18 11:13 PM
 */
public class SparkSqlLineageTest extends AbstractSqlLineageTest {

    protected static final SqlParser SQL_PARSER = new SqlParser();

    @Test
    public void testInsertInto() throws Exception {
        String sql = "with sdfa as (select concat(a.COL1, '-', a.COL2), a.desc,  substr(current_timestamp(),1,19) AS data_store_time " +
                "from db1.test a where ds='201912') insert overwrite table db2.Demo select * from sdfa";
        Statement statement = SQL_PARSER.createStatement(sql);

        Analysis analysis = new Analysis(statement, emptyMap());
        StatementAnalyzer statementAnalyzer = new StatementAnalyzer(analysis, new SimpleSparkMetadataService(), SQL_PARSER);
        
        statementAnalyzer.analyze(statement, Optional.empty());

        //System.out.println(SqlFormatter.formatSql(statement));
        System.out.println(JsonUtils.toJSONString(analysis.getTarget().get()));
    }
}
