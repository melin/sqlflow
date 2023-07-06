package io.github.melin.sqlflow.parser.flink

import io.github.melin.sqlflow.analyzer.Analysis
import io.github.melin.sqlflow.analyzer.StatementAnalyzer
import io.github.melin.sqlflow.metadata.SchemaTable
import io.github.melin.sqlflow.metadata.SimpleMetadataService
import io.github.melin.sqlflow.parser.SqlParser
import io.github.melin.sqlflow.util.JsonUtils
import io.github.melin.superior.common.relational.create.CreateTable
import io.github.melin.superior.common.relational.dml.InsertTable
import io.github.melin.superior.parser.flink.FlinkSqlHelper
import org.assertj.core.util.Lists
import org.junit.Test
import java.util.*

class FlinkSqlLineageTest1 {

    protected val SQL_PARSER = SqlParser()

    @Test
    @Throws(Exception::class)
    fun testInsertInto() {
        val script = """
            CREATE TABLE user_behavior (
                user_id BIGINT,
                item_id BIGINT,
                category_id BIGINT,
                behavior STRING,
                ts TIMESTAMP(3),
                proctime AS PROCTIME(), 
                WATERMARK FOR ts AS ts - INTERVAL '5' SECOND 
            ) WITH (
                'connector' = 'kafka',  -- using kafka connector
                'topic' = 'user_behavior',  -- kafka topic
                'scan.startup.mode' = 'earliest-offset',  -- reading from the beginning
                'properties.bootstrap.servers' = 'kafka:9094',  -- kafka broker address
                'format' = 'json'  -- the data format is json
            );
            
            INSERT INTO buy_cnt_per_hour
            SELECT HOUR(TUMBLE_START(ts, INTERVAL '1' HOUR)), COUNT(*)
            FROM user_behavior
            WHERE behavior = 'buy'
            GROUP BY TUMBLE(ts, INTERVAL '1' HOUR);
        """.trimIndent()

        // sqlflow 只支持 insert as select 语句血缘解析，create table语句需要先解析出元数据信息
        val metadataService = SimpleMetadataService("default");
        val statements = FlinkSqlHelper.parseMultiStatement(script)
        val tables = Lists.newArrayList<SchemaTable>()
        for (statement in statements) {
            if (statement is CreateTable) {
                val columns = statement.columnRels?.map { it.name }
                val table = SchemaTable(
                    "default",
                    statement.tableId.getFullTableName(),
                    columns
                )
                tables.add(table)
            }
        }
        metadataService.addTableMetadata(tables)

        // 实例演示，设置设置表元数据，生产环境定制MetadataService，从元数据系统查询表字段信息。
        val columns = Lists.newArrayList<String>()
        columns.add("hour")
        columns.add("orders_num")
        val table =
            SchemaTable("default", "buy_cnt_per_hour", columns);
        metadataService.addTableMetadata(table)

        for (statement in statements) {
            if (statement is InsertTable) {
                val stat = SQL_PARSER.createStatement(statement.getSql())
                val analysis = Analysis(stat, emptyMap())
                val statementAnalyzer = StatementAnalyzer(
                    analysis,
                    metadataService,
                    SQL_PARSER
                )
                statementAnalyzer.analyze(stat, Optional.empty())

                System.out.println(JsonUtils.toJSONString(analysis.getTarget().get()));
            }
        }
    }
}