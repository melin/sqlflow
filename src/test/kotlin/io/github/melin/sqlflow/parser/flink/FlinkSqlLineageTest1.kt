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
            create table if not exists ods_point_st_telemetry_short_float(
ts string,
r32 double,
iv string,
nt string,
sb string,
bl string,
ov string,
type int,
cot int,
source_address int,
a3_received_ts string,
ip string,
port int,
common_address int,
object_address int
) WITH
              (
                'password' = 'epbw1902',
                'connector' = 'jdbc',
                'table-name' = 'products_1_copy29',
                'sink.parallelism' = '1',
                'url' = 'jdbc:mysql://172.18.1.51:3307/fengchao_sync?useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=utf-8',
                'username' = 'root'
              );

create table if not exists dim_pub_fan_info(
fan_num string comment "工程编号",
fan_cap string comment "容量",
run_num string comment "运行编号",
belong_colt_line string comment "集电线编号",
fan_type string comment "风机型号",
fan_factory string comment "风机厂家",
fan string comment "风机",
fan_num2 string comment "工程编号2"
)WITH
              (
                'password' = 'epbw1902',
                'connector' = 'jdbc',
                'table-name' = 'products_1_copy29',
                'sink.parallelism' = '1',
                'url' = 'jdbc:mysql://172.18.1.51:3307/fengchao_sync?useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=utf-8',
                'username' = 'root'
              );

create table IF NOT EXISTS dim_pub_telemetry_info 
(
     fan_num                    string comment'风机编号'
    ,fan_type                   string comment'风机类型'
    ,msr_point_colt_time  string comment'测点采集时间'        
    ,sec_wind_speed         string comment'秒级风速'
    ,data_store_time        string comment'数据入库时间'  
)WITH
              (
                'password' = 'epbw1902',
                'connector' = 'jdbc',
                'table-name' = 'products_1_copy29',
                'sink.parallelism' = '1',
                'url' = 'jdbc:mysql://172.18.1.51:3307/fengchao_sync?useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=utf-8',
                'username' = 'root'
              );

create table if not exists dim_pub_fan_telemetry_info(
device_name string comment "装置名称",
desc string comment "描述",
obj_addr string comment "对象地址",
factor string comment "乘积系数",
offset1 string comment "偏移量",
unit string comment "单位",
ip string comment "ip地址",
port int comment "端口号"
) WITH
              (
                'password' = 'epbw1902',
                'connector' = 'jdbc',
                'table-name' = 'products_1_copy29',
                'sink.parallelism' = '1',
                'url' = 'jdbc:mysql://172.18.1.51:3307/fengchao_sync?useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=utf-8',
                'username' = 'root'
              );


create table if not exists dwd_wind_fan_wind_resrc(
    fan_num string comment "装置名称",
    fan_cap string comment "描述",
    msr_point_colt_time string comment "对象地址",
    sec_wind_speed string comment "乘积系数",
    data_store_time string comment "偏移量"
) WITH
              (
                'password' = 'epbw1902',
                'connector' = 'jdbc',
                'table-name' = 'products_1_copy29',
                'sink.parallelism' = '1',
                'url' = 'jdbc:mysql://172.18.1.51:3307/fengchao_sync?useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=utf-8',
                'username' = 'root'
              );

insert  overwrite  dwd_wind_fan_wind_resrc partition (dt='1231')
SELECT  t1.fan_num                                              fan_num               --风机编号
        ,t2.fan_cap                                             fan_type              --风机类型
        ,t1.msr_point_colt_time                                 msr_point_colt_time   --测点采集时间
        ,t1.sec_wind_speed                                      sec_wind_speed        --秒级风速
        ,substr(current_timestamp(),1,19)                       data_store_time       --数据入库时间
from
(
  SELECT 
              case 
                when length(ti.desc) = 8 then concat('#0',SUBSTRING(ti.desc,1,1))
                when length(ti.desc) = 9 then concat('#',SUBSTRING(ti.desc,1,2))
                ELSE NULL
              end as fan_num                --风机编号
              ,ts   msr_point_colt_time     --测点采集时间
              ,r32  sec_wind_speed          --秒级风速
  from        ods_point_st_telemetry_short_float as tsf
  left join   dim_pub_fan_telemetry_info ti
              on tsf.ip = ti.ip
  where       ti.desc LIKE '%风机瞬时风速%'   --只取瞬时风速数据
              and tsf.r32 BETWEEN 0 and 50             --对风速大于50m/s的数据则进行去除
              and dt=from_unixtime(unix_timestamp('1231','yyyymmdd'),'yyyy-mm-dd')
)t1
left join   dim_pub_fan_info t2
            on t1.fan_num = t2.fan_num
;
        """.trimIndent()

        // sqlflow 只支持 insert as select 语句血缘解析，create table语句需要先解析出元数据信息
        val metadataService = SimpleMetadataService("default");
        val statements = FlinkSqlHelper.parseMultiStatement(script)
        val tables = Lists.newArrayList<SchemaTable>()
        for (statement in statements) {
            if (statement is CreateTable) {
                val columns = statement.columnRels?.map { it.columnName }
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