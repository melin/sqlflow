package com.github.melin.sqlflow.parser.gauss;

import com.github.melin.sqlflow.analyzer.Analysis;
import com.github.melin.sqlflow.analyzer.OutputColumn;
import com.github.melin.sqlflow.analyzer.StatementAnalyzer;
import com.github.melin.sqlflow.metadata.Column;
import com.github.melin.sqlflow.metadata.QualifiedObjectName;
import com.github.melin.sqlflow.parser.AbstractSqlLineageTest;
import com.github.melin.sqlflow.parser.SqlParser;
import com.github.melin.sqlflow.tree.NodeLocation;
import com.github.melin.sqlflow.tree.statement.Statement;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;
import java.util.Set;

import static java.util.Collections.emptyMap;

/**
 * huaixin 2021/12/18 11:13 PM
 */
public class GausssSqlLineageTest extends AbstractSqlLineageTest {

    protected static final SqlParser SQL_PARSER = new SqlParser();

    @Test
    public void testInsertInto() throws Exception {
        String sql = "insert into demo select * from (select concat(a.col1, '-', a.col2), a.row_num + cast(a.ds as bigint) from test a where ds='201912') b";
        Statement statement = SQL_PARSER.createStatement(sql);

        Analysis analysis = new Analysis(statement, emptyMap());
        StatementAnalyzer statementAnalyzer = new StatementAnalyzer(analysis, new SimpleGaussMetadataService(), SQL_PARSER);
        
        statementAnalyzer.analyze(statement, Optional.empty());

        //System.out.println(MapperUtils.toJSONString(analysis.getTarget().get()));

        assertLineage(analysis, new OutputColumn(new Column("name", "varchar"), ImmutableSet.of(
                new Analysis.SourceColumn(QualifiedObjectName.valueOf("default.bigdata.test"), "col1"),
                new Analysis.SourceColumn(QualifiedObjectName.valueOf("default.bigdata.test"), "col2")
        )), new OutputColumn(new Column("row_num", "bigint"), ImmutableSet.of(
                new Analysis.SourceColumn(QualifiedObjectName.valueOf("default.bigdata.test"), "row_num"),
                new Analysis.SourceColumn(QualifiedObjectName.valueOf("default.bigdata.test"), "ds")
        )));

        // ds 在两个位置出现
        Set<NodeLocation> fields = analysis.getOriginField(new Analysis.SourceColumn(QualifiedObjectName.valueOf("default.bigdata.test"), "ds"));
        Assert.assertEquals(2, fields.size());
    }
}
