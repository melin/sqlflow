package io.github.melin.sqlflow.parser.flink

import io.github.melin.sqlflow.analyzer.Analysis
import io.github.melin.sqlflow.analyzer.StatementAnalyzer
import io.github.melin.sqlflow.metadata.SchemaTable
import io.github.melin.sqlflow.metadata.SimpleMetadataService
import io.github.melin.sqlflow.parser.SqlParser
import io.github.melin.sqlflow.util.JsonUtils
import io.github.melin.superior.common.relational.create.CreateTable
import io.github.melin.superior.common.relational.dml.InsertTable
import io.github.melin.superior.parser.spark.SparkSqlHelper
import org.assertj.core.util.Lists
import org.junit.Test
import java.util.*

class FlinkSqlLineageTest2 {

    protected val SQL_PARSER = SqlParser()

    @Test
    @Throws(Exception::class)
    fun testInsertInto() {
        val script = """
            CREATE TABLE ods_pms_rm_fan_start_stop_record (
                id INT,
                wind_number VARCHAR(512),
                wind_name FLOAT,
                fan_name INT,
                halt_stop_time INT,
                halt_start_time INT, 
                maintain_start_time INT,
                shutdown_fault_type_id INT,
                shutdown_fault_type_name INT,
                shutdown_fault_type_relation_name INT,
                halt_hour INT,
                plan_type INT,
                create_time INT,
                update_time INT
              )
            tblproperties (
                'jdbc.properties.useSSL' = 'false'
              );
              
           CREATE TABLE dim_pub_shutdown_fault_type(
                shutdown_type_name VARCHAR(255),
                plan_type VARCHAR(512),
                id INT
              )
            tblproperties (
                'jdbc.properties.useSSL' = 'false'
              );
            
            CREATE TABLE dwd_stat_fan_start_stop_record_src(
                id VARCHAR(255),
                wind_number VARCHAR(512),
                wind_name FLOAT,
                plan_type_name STRING,
                fan_num INT,
                halt_start_time INT, 
                real_halt_stop_time INT,
                halt_stop_time INT,
                maintain_start_time INT,
                shutdown_fault_type_id INT,
                shutdown_fault_type_name INT,
                shutdown_fault_type_relation_name INT,
                halt_hour INT,
                plan_type INT,
                create_time INT,
                update_time INT
              )
            tblproperties (
                'password' = 'epbw1902'
              );
            
            insert overwrite TABLE dwd_stat_fan_start_stop_record_src
            SELECT
              ods_pms_rm_fan_start_stop_record.id,
              wind_number,
              wind_name,
              e.plan_type plan_type_name,
              case
                when length(fan_name) = 10 then concat('#0', SUBSTRING(fan_name, 7, 1))
                when length(fan_name) = 11 then concat('#', SUBSTRING(fan_name, 7, 2))
                ELSE NULL
              end as fan_num,
              from_unixtime (
                unix_timestamp(halt_start_time, 'yyyy-MM-dd HH:mm:ss'),
                'yyyy-MM-dd HH:mm:ss'
              ) halt_start_time,
              halt_stop_time real_halt_stop_time,
              case
                when halt_stop_time is null or halt_stop_time ='' then concat(substr(current_timestamp(),1,10),' 00:00:00')
                ELSE from_unixtime (
                  unix_timestamp(halt_stop_time, 'yyyy-MM-dd HH:mm:ss'),
                  'yyyy-MM-dd HH:mm:ss'
                )
              end as halt_stop_time
              ,maintain_start_time
              ,shutdown_fault_type_id
              ,shutdown_fault_type_name
              ,shutdown_fault_type_relation_name
              ,halt_hour
              ,ods_pms_rm_fan_start_stop_record.plan_type
              ,create_time
              ,update_time
            from
              ods_pms_rm_fan_start_stop_record 
            left join
              (
                  SELECT 
                         shutdown_type_name
                        ,case 
                              when plan_type = 0 then '非计划停机'
                              when plan_type = 1 then '计划停机'
                              when plan_type = 2 then '受累计划停运'
                              when plan_type = 3 then '调度限功率'
                              else null
                        end as plan_type
                        ,id
                  from dim_pub_shutdown_fault_type
                ) e
            on ods_pms_rm_fan_start_stop_record.shutdown_fault_type_id = e.id
            where
              fan_name LIKE '%风机'
              AND is_del = '0' 
        """.trimIndent()

        // sqlflow 只支持 insert as select 语句血缘解析，create table语句需要先解析出元数据信息
        val metadataService = SimpleMetadataService("fengchao_sync");
        val statements = SparkSqlHelper.parseMultiStatement(script)
        val tables = Lists.newArrayList<SchemaTable>()
        for (statement in statements) {
            if (statement is CreateTable) {
                val columns = statement.columnRels?.map { it.columnName }
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