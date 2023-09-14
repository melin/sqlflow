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
public class SparkSqlLineageTest1 extends AbstractSqlLineageTest {

    protected static final SqlParser SQL_PARSER = new SqlParser();

    @Test
    public void testCreateView() throws Exception {
        String sql = "CREATE VIEW IF NOT EXISTS `MDM_VIEW_PRODUCT_ENRICHMENT_TRANSLATE` AS\n" +
                "\n" +
                " select concat(a.col1, '-', a.col2) tez, a.row_num from db1.test a where ds='201912'";
        Statement statement = SQL_PARSER.createStatement(sql);

        Analysis analysis = new Analysis(statement, emptyMap());
        StatementAnalyzer statementAnalyzer = new StatementAnalyzer(analysis, new SimpleSparkMetadataService(), SQL_PARSER);
        
        statementAnalyzer.analyze(statement, Optional.empty());

        //System.out.println(SqlFormatter.formatSql(statement));
        System.out.println(JsonUtils.toJSONString(analysis.getTarget().get()));
    }
}
