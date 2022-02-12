package com.github.melin.sqlflow.parser.presto;

import com.github.melin.sqlflow.DbType;
import com.github.melin.sqlflow.analyzer.Analysis;
import com.github.melin.sqlflow.analyzer.OutputColumn;
import com.github.melin.sqlflow.analyzer.StatementAnalyzer;
import com.github.melin.sqlflow.formatter.SqlFormatter;
import com.github.melin.sqlflow.metadata.Column;
import com.github.melin.sqlflow.metadata.QualifiedObjectName;
import com.github.melin.sqlflow.parser.AbstractSqlLineageTest;
import com.github.melin.sqlflow.parser.SqlParser;
import com.github.melin.sqlflow.tree.statement.Statement;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.util.Optional;

import static java.util.Collections.emptyMap;

/**
 * huaixin 2021/12/18 11:13 PM
 */
public class PrestoSqlLineageTest extends AbstractSqlLineageTest {

    protected static final SqlParser SQL_PARSER = new SqlParser(DbType.PRESTO);

    @Test
    public void testInsertInto() throws Exception {
        String sql = "insert into demo select concat(a.col1, '-', a.col2), sum(a.row_num) as num from test a where ds='201912' group by type";
        Statement statement = SQL_PARSER.createStatement(sql);

        Analysis analysis = new Analysis(statement, emptyMap());
        StatementAnalyzer statementAnalyzer = new StatementAnalyzer(analysis, new SimplePrestoMetadata(), SQL_PARSER);
        
        statementAnalyzer.analyze(statement, Optional.empty());

        System.out.println(SqlFormatter.formatSql(statement));
        //System.out.println(MapperUtils.toJSONString(analysis.getTarget().get()));

        assertLineage(analysis, new OutputColumn(new Column("name", "varchar"), ImmutableSet.of(
                new Analysis.SourceColumn(QualifiedObjectName.valueOf("default.bigdata.test"), "col1"),
                new Analysis.SourceColumn(QualifiedObjectName.valueOf("default.bigdata.test"), "col2")
        )), new OutputColumn(new Column("row_num", "bigint"), ImmutableSet.of(
                new Analysis.SourceColumn(QualifiedObjectName.valueOf("default.bigdata.test"), "row_num")
        )));
    }
}
