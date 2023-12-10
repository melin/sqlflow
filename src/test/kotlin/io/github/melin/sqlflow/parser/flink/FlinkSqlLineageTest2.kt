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
            CREATE TABLE dws_res_fan_d (
                biz_date INT,
                fan_num VARCHAR(512),
                avg_wind_speed FLOAT,
                avg_efftv_wind_speed INT,
                turblc_intst INT,
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
              
           CREATE TABLE dwd_res_fan_avg_res_5min(
                biz_date INT,
                fan_num VARCHAR(512),
                avg_wind_speed FLOAT,
                avg_efftv_wind_speed INT,
                turblc_intst INT,
                avg_wind_speed_rec_num INT,
                avg_efftv_wind_speed_rec_num INT,
                wind_speed_stdea INT,
                dt INT
              )
            tblproperties (
                'jdbc.properties.useSSL' = 'false'
              );
            
            CREATE TABLE dws_res_fan_h(
                biz_date INt,
                efftv_wind_speed_num VARCHAR(255),
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
            
insert overwrite table dws_res_fan_d partition(dt='${'$'}{yyyymmdd}')
select 
  t1.biz_date as biz_date,
  t1.fan_num as fan_num,
  t1.avg_wind_speed as avg_wind_speed,
  t1.avg_efftv_wind_speed as avg_efftv_wind_speed,
  t2.turblc_intst as avg_turblc_intst,
  t3.efftv_wind_speed_num as efftv_wind_speed_num
from
(
-- 风机日平均风速,平均有效风速
  select 
    a.biz_date as biz_date,
    a.fan_num as fan_num,
    a.avg_wind_speed / a.avg_wind_speed_rec_num as avg_wind_speed,
    a.avg_efftv_wind_speed / a.avg_efftv_wind_speed_rec_num as avg_efftv_wind_speed
  from 
  (
    select
      substr(biz_date,0,10) as biz_date,
      fan_num,
      sum(avg_wind_speed * avg_wind_speed_rec_num) as avg_wind_speed,
      sum(avg_wind_speed_rec_num) as avg_wind_speed_rec_num,
      sum(avg_efftv_wind_speed * avg_efftv_wind_speed_rec_num) as avg_efftv_wind_speed,
      sum(avg_efftv_wind_speed_rec_num) as avg_efftv_wind_speed_rec_num
    from dwd_res_fan_avg_res_5min
    where dt = '${'$'}{yyyymmdd}' and biz_date is not null
    group by substr(biz_date,0,10),fan_num
  ) as a
) as t1
left join 
(
-- 风机日平均湍流强度
  select 
    substr(a.biz_date,0,10) as biz_date,
    a.fan_num,
    avg(a.turblc_intst) as  turblc_intst
  from 
  (
    select 
      biz_date,
      fan_num,
      (wind_speed_stdea / avg_wind_speed) as turblc_intst,
      dt
    from dwd_res_fan_avg_res_5min
    where dt = '${'$'}{yyyymmdd}'
  ) as a 
  group by substr(a.biz_date,0,10),a.fan_num
) as t2
on t1.biz_date = t2.biz_date
and t1.fan_num = t2.fan_num
left join 
(
-- 有效风速数
  select
    substr(biz_date,0,10) as biz_date,
    fan_num,
    count(1) as efftv_wind_speed_num
  from dws_res_fan_h
  where dt = '${'$'}{yyyymmdd}' and 3 < avg_wind_speed and avg_wind_speed < 25
  group by substr(biz_date,0,10),fan_num
) as t3
on t1.biz_date = t3.biz_date
and t1.fan_num = t3.fan_num
order by t1.biz_date,cast(substr(t1.fan_num,2,3) as int);

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