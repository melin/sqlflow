解析sql 获取表和列血缘关系，以及算子信息，代码是从 trino 中抽取的，trino sql语法支持比较全。实现其它数据库sql血缘解析，适当调整antlr4 语法文件，比较容易实现。
目前支持spark、hive、flink、trino 等sql

### 主要功能:
1. 表与表血缘
2. 列与列血缘
3. sql 代码格式


### example
Refer: [FlinkSqlLineageTest2.kt](src%2Ftest%2Fkotlin%2Fio%2Fgithub%2Fmelin%2Fsqlflow%2Fparser%2Fflink%2FFlinkSqlLineageTest2.kt)
```
insert overwrite table dws_res_fan_d partition(dt='{yyyymmdd}')
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
    where dt = '${yyyymmdd}' and biz_date is not null
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
    where dt = '${yyyymmdd}'
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
  where dt = '${yyyymmdd}' and 3 < avg_wind_speed and avg_wind_speed < 25
  group by substr(biz_date,0,10),fan_num
) as t3
on t1.biz_date = t3.biz_date
and t1.fan_num = t3.fan_num
order by t1.biz_date,cast(substr(t1.fan_num,2,3) as int);
```

解析输出血缘信息。
```
{
  "catalogName" : null,
  "schema" : "ods_sync",
  "table" : "dws_res_fan_d",
  "columns" : [ {
    "column" : "biz_date",
    "sourceColumns" : [ {
      "tableName" : "ods_sync.dwd_res_fan_avg_res_5min",
      "columnName" : "biz_date"
    } ]
  }, {
    "column" : "fan_num",
    "sourceColumns" : [ {
      "tableName" : "ods_sync.dwd_res_fan_avg_res_5min",
      "columnName" : "fan_num"
    } ]
  }, {
    "column" : "avg_wind_speed",
    "sourceColumns" : [ {
      "tableName" : "ods_sync.dwd_res_fan_avg_res_5min",
      "columnName" : "avg_wind_speed"
    }, {
      "tableName" : "ods_sync.dwd_res_fan_avg_res_5min",
      "columnName" : "avg_wind_speed_rec_num"
    } ]
  }, {
    "column" : "avg_efftv_wind_speed",
    "sourceColumns" : [ {
      "tableName" : "ods_sync.dwd_res_fan_avg_res_5min",
      "columnName" : "avg_efftv_wind_speed"
    }, {
      "tableName" : "ods_sync.dwd_res_fan_avg_res_5min",
      "columnName" : "avg_efftv_wind_speed_rec_num"
    } ]
  }, {
    "column" : "turblc_intst",
    "sourceColumns" : [ {
      "tableName" : "ods_sync.dwd_res_fan_avg_res_5min",
      "columnName" : "wind_speed_stdea"
    }, {
      "tableName" : "ods_sync.dwd_res_fan_avg_res_5min",
      "columnName" : "avg_wind_speed"
    } ]
  }, {
    "column" : "halt_start_time",
    "sourceColumns" : [ ]
  } ]
}
```