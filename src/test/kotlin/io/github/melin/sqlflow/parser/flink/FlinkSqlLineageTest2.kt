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

class FlinkSqlLineageTest2 {

    protected val SQL_PARSER = SqlParser()

    @Test
    @Throws(Exception::class)
    fun testInsertInto() {
        val script = """
            CREATE TABLE
              SOURCE_PRODUCTS(
                name VARCHAR(255),
                description VARCHAR(512),
                weight FLOAT,
                id INT,
                PRIMARY KEY (id) NOT ENFORCED
              )
            WITH
              (
                'jdbc.properties.useSSL' = 'false',
                'hostname' = '172.18.1.51',
                'password' = 'epbw1902',
                'jdbc.properties.characterEncoding' = 'utf-8',
                'connector' = 'mysql-cdc',
                'port' = '3307',
                'database-name' = 'fengchao_sync',
                'table-name' = 'products_1_copy19',
                'jdbc.properties.serverTimezone' = 'Asia/Shanghai',
                'username' = 'root'
              );
            
            CREATE TABLE
              SINK_PRODUCTS(
                name VARCHAR(255),
                description VARCHAR(512),
                weight FLOAT,
                id INT,
                PRIMARY KEY (id) NOT ENFORCED
              )
            WITH
              (
                'password' = 'epbw1902',
                'connector' = 'jdbc',
                'table-name' = 'products_1_copy29',
                'sink.parallelism' = '1',
                'url' = 'jdbc:mysql://172.18.1.51:3307/fengchao_sync?useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=utf-8',
                'username' = 'root'
              );
            
            insert into
              SINK_PRODUCTS
            select
              *
            from
              SOURCE_PRODUCTS;
        """.trimIndent()

        // sqlflow 只支持 insert as select 语句血缘解析，create table语句需要先解析出元数据信息
        val metadataService = SimpleMetadataService("fengchao_sync");
        val statements = FlinkSqlHelper.parseMultiStatement(script)
        val tables = Lists.newArrayList<SchemaTable>()
        for (statement in statements) {
            if (statement is CreateTable) {
                val columns = statement.columnRels?.map { it.name }
                val table = SchemaTable(
                    "fengchao_sync",
                    statement.tableId.getFullTableName(),
                    columns
                )
                tables.add(table)
            }
        }
        metadataService.addTableMetadata(tables)

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