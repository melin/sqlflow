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
           WITH TMP_HA02_MARKET_PROFIT_ANALYSE_MOBIE_ALL as
(
select 
CASE  WHEN a.GROUP_NAME IS NOT NULL AND a.AREA_GUID IS  NULL AND a.COMPANY_GUID IS NULL AND a.PROJECT_GUID IS NULL  THEN '总部'
        	 WHEN a.GROUP_NAME IS NOT NULL AND a.AREA_GUID IS NOT NULL AND a.COMPANY_GUID IS NULL AND a.PROJECT_GUID IS NULL  THEN '区域'
	         WHEN a.GROUP_NAME IS NOT NULL AND a.AREA_GUID IS NOT NULL AND a.COMPANY_GUID IS NOT NULL AND  a.PROJECT_GUID IS NULL      THEN '公司'
			 WHEN a.GROUP_NAME IS NOT NULL AND a.AREA_GUID IS NOT NULL AND a.COMPANY_GUID IS NOT NULL AND  a.PROJECT_GUID IS NOT NULL  THEN '项目'	
			 ELSE '存在错误数据' END     AS ORG_TYPE 
,CASE  WHEN a.GROUP_NAME IS NOT NULL AND a.AREA_GUID IS  NULL AND a.COMPANY_GUID IS NULL AND a.PROJECT_GUID IS NULL  THEN '11B11DB4-E907-4F1F-8835-B9DAAB6E1F23'
        	 WHEN a.GROUP_NAME IS NOT NULL AND a.AREA_GUID IS NOT NULL AND a.COMPANY_GUID IS NULL AND a.PROJECT_GUID IS NULL  THEN a.AREA_GUID
	         WHEN a.GROUP_NAME IS NOT NULL AND a.AREA_GUID IS NOT NULL AND a.COMPANY_GUID IS NOT NULL AND  a.PROJECT_GUID IS NULL      THEN  a.COMPANY_GUID 
			 WHEN a.GROUP_NAME IS NOT NULL AND a.AREA_GUID IS NOT NULL AND a.COMPANY_GUID IS NOT NULL AND  a.PROJECT_GUID IS NOT NULL  THEN a.PROJECT_GUID	
			 ELSE '存在错误数据' END     AS ORG_GUID  
  ,a.M_YEARMONTH M_YEARMONTH -- 年月
  ,a.M_YEAR      M_YEAR -- 年份
  ,a.M_MONTH     M_MONTH -- 月份
  ,a.glkj		 MANAG_CALIBER	  -- 管理口径
  ,sum(case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then a.SOLD_QY_SALESPROFIT_Y/10000 else a.SOLD_SALESPROFIT_Y/10000 end ) 	as SOLD_SALESPROFIT_Y       -- 本年已售销售净利润
  ,sum(case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then a.SOLD_QY_SALE_INCOME_Y/10000 else a.SOLD_SALE_INCOME_Y/10000 end) 	as SOLD_SALE_INCOME_Y		--年已售累计销售收入
  ,0						as SOLD_SALESPROFIT_TARGET_Y				-- 本年已售销售净利润目标
  ,0						as SOLD_SALE_INCOME_TARGET_Y				-- 本年已售累计销售收入目标
  ,case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then sum(a.SOLD_QY_SALESPROFIT_Y)/sum(a.SOLD_QY_SALE_INCOME_Y)*100 else sum(a.SOLD_SALESPROFIT_Y)/sum(a.SOLD_SALE_INCOME_Y)*100 end as SOLD_SALESPROFIT_RATE_Y	-- 本年已售销售净利率
  ,0						as SOLD_SALESPROFIT_RATE_TARGET_Y			-- 本年已售销售净利率目标
  ,sum(case when  a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then (case when a.M_YEARMONTH=substring(add_months(current_date,-1),1,7) then a.SOLD_QY_SALESPROFIT_Y/10000 else 0 end) -
	(case when a.M_YEARMONTH=substring(add_months(current_date,-13),1,7) then a.SOLD_QY_SALESPROFIT_Y else 0 end)/10000
else 	(case when a.M_YEARMONTH=substring(add_months(current_date,-1),1,7) then a.SOLD_SALESPROFIT_Y/10000 else 0 end) -
	(case when a.M_YEARMONTH=substring(add_months(current_date,-13),1,7) then a.SOLD_SALESPROFIT_Y else 0 end)/10000 end) as	SOLD_SALESPROFIT_Y2Y					-- 本年已售销售净利润同比
  ,case when  a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then (case when a.M_YEARMONTH=substring(add_months(current_date,-1),1,7) then sum(a.SOLD_QY_SALESPROFIT_Y)/sum(a.SOLD_QY_SALE_INCOME_Y) else 0 end)*100 -
	(case when a.M_YEARMONTH=substring(add_months(current_date,-13),1,7) then sum(a.SOLD_QY_SALESPROFIT_Y)/sum(a.SOLD_QY_SALE_INCOME_Y) else 0 end)*100	
	else (case when a.M_YEARMONTH=substring(add_months(current_date,-1),1,7) then sum(a.SOLD_SALESPROFIT_Y)/sum(a.SOLD_SALE_INCOME_Y) else 0 end)*100 -
	(case when a.M_YEARMONTH=substring(add_months(current_date,-13),1,7) then sum(a.SOLD_SALESPROFIT_Y)/sum(a.SOLD_SALE_INCOME_Y) else 0 end)*100 end as	SOLD_SALESPROFIT_RATE_Y2Y-- 本年已售销售净利率同比
  ,0						as	SOLD_SALESPROFIT_PC_Y					-- 本年已售销售净利润完成率
  ,0						as	SOLD_SALESPROFIT_PCT_Y					-- 本年已售销售净利率偏差pct
  ,0						as	SOLD_SALESPROFIT_GAP_Y					-- 本年已售销售净利润缺口
  ,sum(case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then a.SOLD_QY_AMOUNT_Y/10000 else a.SOLD_AMOUNT_Y/10000 end) 	as SOLD_AMOUNT_Y           -- 本年已售金额
  ,sum(case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then a.SOLD_QY_COST_Y/10000 else a.SOLD_COST_Y/10000 end) 		as SOLD_COST_Y         		-- 本年已售成本
  ,case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then sum(SOLD_QY_SALESPROFIT_Y)/sum(SOLD_QY_COST_Y)*100 
  else sum(SOLD_SALESPROFIT_Y)/sum(SOLD_COST_Y)*100  end as			SOLD_COSTPROFIT_RATE_Y	-- 本年已售成本利润率
  ,case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then (case when a.M_YEARMONTH=substring(add_months(current_date,-1),1,7) then sum(a.SOLD_QY_SALESPROFIT_Y)/sum(a.SOLD_QY_COST_Y) else 0 end)*100 -
	(case when a.M_YEARMONTH=substring(add_months(current_date,-13),1,7) then sum(a.SOLD_QY_SALESPROFIT_Y)/sum(a.SOLD_QY_COST_Y) else 0 end)*100 
	else (case when a.M_YEARMONTH=substring(add_months(current_date,-1),1,7) then sum(a.SOLD_SALESPROFIT_Y)/sum(a.SOLD_COST_Y) else 0 end)*100 -
	(case when a.M_YEARMONTH=substring(add_months(current_date,-13),1,7) then sum(a.SOLD_SALESPROFIT_Y)/sum(a.SOLD_COST_Y) else 0 end)*100 
	end as     SOLD_COSTPROFIT_RATE_Y2Y	-- 本年已售成本利润率同比
  ,sum(case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then a.SOLD_QY_SALESPROFIT_M/10000 else a.SOLD_SALESPROFIT_M/10000 end) SOLD_SALESPROFIT_M						-- 本月已售销售净利润
  ,case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then sum(a.SOLD_QY_SALESPROFIT_M)/sum(a.SOLD_QY_SALE_INCOME_M)*100	
	else  sum(a.SOLD_SALESPROFIT_M)/sum(a.SOLD_SALE_INCOME_M)*100 end SOLD_SALESPROFIT_RATE_M				-- 本月已售销售净利率
  ,case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then sum(a.SOLD_QY_SALESPROFIT_M)/sum(a.SOLD_QY_COST_M)*100	
else sum(a.SOLD_SALESPROFIT_M)/sum(a.SOLD_COST_M)*100 end   SOLD_COSTPROFIT_RATE_M				-- 本月已售成本利润率
  ,sum(case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then a.SOLD_QY_SALESPROFIT_M_1/10000 else a.SOLD_SALESPROFIT_M_1/10000 end) SOLD_SALESPROFIT_M_1					-- T-1月已售销售净利润
  ,case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then  sum(a.SOLD_QY_SALESPROFIT_M_1)/sum(a.SOLD_QY_SALE_INCOME_M_1)*100		
else  sum(a.SOLD_SALESPROFIT_M_1)/sum(a.SOLD_SALE_INCOME_M_1)*100 end as  SOLD_SALESPROFIT_RATE_M_1	-- T-1月已售销售净利率
  ,case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then sum(a.SOLD_QY_SALESPROFIT_M_1)/sum(a.SOLD_QY_COST_M_1)*100	
else sum(a.SOLD_SALESPROFIT_M_1)/sum(a.SOLD_COST_M_1)*100 end as  SOLD_COSTPROFIT_RATE_M_1	-- T-1月已售成本利润率
  ,sum(case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then a.SOLD_QY_SALESPROFIT_M_2/10000 else a.SOLD_SALESPROFIT_M_2/10000 end)  	SOLD_SALESPROFIT_M_2				-- T-2月已售销售净利润
  ,case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then sum(a.SOLD_QY_SALESPROFIT_M_2)/sum(a.SOLD_QY_SALE_INCOME_M_2)*100	
else  sum(a.SOLD_SALESPROFIT_M_2)/sum(a.SOLD_SALE_INCOME_M_2)*100 end as  SOLD_SALESPROFIT_RATE_M_2		-- T-2月已售销售净利率
  ,case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then sum(a.SOLD_QY_SALESPROFIT_M_2)/sum(a.SOLD_QY_COST_M_2)*100	
else sum(a.SOLD_SALESPROFIT_M_2)/sum(a.SOLD_COST_M_2)*100 end as  SOLD_COSTPROFIT_RATE_M_2		-- T-2月已售成本利润率
  ,sum(case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then a.SOLD_QY_SALESPROFIT_M_3/10000 else a.SOLD_SALESPROFIT_M_3/10000 end)  SOLD_SALESPROFIT_M_3					-- T-3月已售销售净利润
  ,case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then sum(a.SOLD_QY_SALESPROFIT_M_3)/sum(a.SOLD_QY_SALE_INCOME_M_3)*100
else sum(a.SOLD_SALESPROFIT_M_3)/sum(a.SOLD_SALE_INCOME_M_3)*100 end as  SOLD_SALESPROFIT_RATE_M_3		-- T-3月已售销售净利率
  ,case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then sum(a.SOLD_QY_SALESPROFIT_M_3)/sum(a.SOLD_QY_COST_M_3)*100	
else sum(a.SOLD_SALESPROFIT_M_3)/sum(a.SOLD_COST_M_3)*100 end as  SOLD_COSTPROFIT_RATE_M_3		-- T-3月已售成本利润率
  ,sum(case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then a.SOLD_QY_SALESPROFIT_M_4/10000 else a.SOLD_SALESPROFIT_M_4/10000 end) 	SOLD_SALESPROFIT_M_4				-- T-4月已售销售净利润
  ,case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then sum(a.SOLD_QY_SALESPROFIT_M_4)/sum(a.SOLD_QY_SALE_INCOME_M_4)*100
else sum(a.SOLD_SALESPROFIT_M_4)/sum(a.SOLD_SALE_INCOME_M_4)*100 end as  SOLD_SALESPROFIT_RATE_M_4		-- T-4月已售销售净利率
  ,case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then sum(a.SOLD_QY_SALESPROFIT_M_4)/sum(a.SOLD_QY_COST_M_4)*100	
else sum(a.SOLD_SALESPROFIT_M_4)/sum(a.SOLD_COST_M_4)*100 end as  SOLD_COSTPROFIT_RATE_M_4		-- T-4月已售成本利润率
  ,sum(case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then a.SOLD_QY_SALESPROFIT_M_5/10000 else a.SOLD_SALESPROFIT_M_5/10000 end)  	SOLD_SALESPROFIT_M_5				-- T-5月已售销售净利润
  ,case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then sum(a.SOLD_QY_SALESPROFIT_M_5)/sum(a.SOLD_QY_SALE_INCOME_M_5)*100
else sum(a.SOLD_SALESPROFIT_M_5)/sum(a.SOLD_SALE_INCOME_M_5)*100 end as  SOLD_SALESPROFIT_RATE_M_5		-- T-5月已售销售净利率
  ,case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then sum(a.SOLD_QY_SALESPROFIT_M_5)/sum(a.SOLD_QY_COST_M_5)*100	
else sum(a.SOLD_SALESPROFIT_M_5)/sum(a.SOLD_COST_M_5)*100 end  as SOLD_COSTPROFIT_RATE_M_5		-- T-5月已售成本利润率
  ,sum(case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then a.UNSOLD_QY_SALESPROFIT_Y/10000 else a.UNSOLD_SALESPROFIT_Y/10000 end)  	as UNSOLD_SALESPROFIT_Y          			-- 本年未售销售净利润
  ,sum(case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then a.UNSOLD_QY_SALE_INCOME_Y/10000 else a.UNSOLD_SALE_INCOME_Y/10000 end)  	as UNSOLD_SALE_INCOME_Y						-- 本年未售累计销售收入
  ,case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then sum(a.UNSOLD_QY_SALESPROFIT_Y)/sum(a.UNSOLD_QY_SALE_INCOME_Y)*100 
else sum(a.UNSOLD_SALESPROFIT_Y)/sum(a.UNSOLD_SALE_INCOME_Y)*100 end   as	UNSOLD_SALESPROFIT_RATE_Y	-- 本年未售销售净利润率
  ,sum(case when  a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then (case when a.M_YEARMONTH=substring(add_months(current_date,-1),1,7) then a.UNSOLD_QY_SALESPROFIT_Y else 0 end)/10000 -
	(case when a.M_YEARMONTH=substring(add_months(current_date,-13),1,7) then a.UNSOLD_QY_SALESPROFIT_Y else 0 end)/10000
else 	(case when a.M_YEARMONTH=substring(add_months(current_date,-1),1,7) then a.UNSOLD_SALESPROFIT_Y else 0 end)/10000 -
	(case when a.M_YEARMONTH=substring(add_months(current_date,-13),1,7) then a.UNSOLD_SALESPROFIT_Y else 0 end) end ) as UNSOLD_SALESPROFIT_Y2Y					-- 本年未售销售净利润同比
  ,case when  a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then (case when a.M_YEARMONTH=substring(add_months(current_date,-1),1,7) then sum(a.UNSOLD_QY_SALESPROFIT_Y)/sum(a.UNSOLD_QY_SALE_INCOME_Y) else 0 end)*100 -
	(case when a.M_YEARMONTH=substring(add_months(current_date,-13),1,7) then sum(a.UNSOLD_QY_SALESPROFIT_Y)/sum(a.UNSOLD_QY_SALE_INCOME_Y) else 0 end)*100 
	else (case when a.M_YEARMONTH=substring(add_months(current_date,-1),1,7) then sum(a.UNSOLD_SALESPROFIT_Y)/sum(a.UNSOLD_SALE_INCOME_Y) else 0 end)*100 -
	(case when a.M_YEARMONTH=substring(add_months(current_date,-13),1,7) then sum(a.UNSOLD_SALESPROFIT_Y)/sum(a.UNSOLD_SALE_INCOME_Y) else 0 end)*100 end as  UNSOLD_SALESPROFIT_RATE_Y2Y	-- 本年未售销售净利润率同比
  ,sum(case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then a.UNSOLD_QY_SALESPROFIT_Y_0/10000 else a.UNSOLD_SALESPROFIT_Y_0/10000 end)  	as UNSOLD_SALESPROFIT_Y_0		-- 拿地年份T-0年未售销售净利润
  ,case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then  sum(a.UNSOLD_QY_SALESPROFIT_Y_0)/sum(a.UNSOLD_QY_SALE_INCOME_Y_0)*100	
else sum(a.UNSOLD_SALESPROFIT_Y_0)/sum(a.UNSOLD_SALE_INCOME_Y_0)*100 end  as UNSOLD_SALESPROFIT_RATE_Y_0	-- 拿地年份T-0年未售销售净利润率
  ,sum(case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then a.UNSOLD_QY_SALESPROFIT_Y_1/10000 else a.UNSOLD_SALESPROFIT_Y_1/10000 end)  	as UNSOLD_SALESPROFIT_Y_1		-- 拿地年份T-1年未售销售净利润
  ,case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then  sum(a.UNSOLD_QY_SALESPROFIT_Y_1)/sum(a.UNSOLD_QY_SALE_INCOME_Y_1)*100	
else sum(a.UNSOLD_SALESPROFIT_Y_1)/sum(a.UNSOLD_SALE_INCOME_Y_1)*100 end  as UNSOLD_SALESPROFIT_RATE_Y_1	-- 拿地年份T-1年未售销售净利润率
  ,sum(case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then a.UNSOLD_QY_SALESPROFIT_Y_2/10000 else a.UNSOLD_SALESPROFIT_Y_2/10000 end)  	as UNSOLD_SALESPROFIT_Y_2		-- 拿地年份T-2年未售销售净利润
  ,case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then sum(a.UNSOLD_QY_SALESPROFIT_Y_2)/sum(a.UNSOLD_QY_SALE_INCOME_Y_2)*100	
else sum(a.UNSOLD_SALESPROFIT_Y_2)/sum(a.UNSOLD_SALE_INCOME_Y_2)*100 end   as UNSOLD_SALESPROFIT_RATE_Y_2	-- 拿地年份T-2年未售销售净利润率
  ,sum(case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then a.UNSOLD_QY_SALESPROFIT_Y_3/10000 else a.UNSOLD_SALESPROFIT_Y_3/10000 end)  	as UNSOLD_SALESPROFIT_Y_3		-- 拿地年份T-3年未售销售净利润
  ,case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then  sum(a.UNSOLD_QY_SALESPROFIT_Y_3)/sum(a.UNSOLD_QY_SALE_INCOME_Y_3)*100 
else  sum(a.UNSOLD_SALESPROFIT_Y_3)/sum(a.UNSOLD_SALE_INCOME_Y_3)*100 end as UNSOLD_SALESPROFIT_RATE_Y_3	-- 拿地年份T-3年未售销售净利润率
  ,sum(case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then a.UNSOLD_QY_SALESPROFIT_Y_4/10000 else a.UNSOLD_SALESPROFIT_Y_4/10000 end)  	as UNSOLD_SALESPROFIT_Y_4		-- 拿地年份T-4年未售销售净利润
  ,case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then  sum(a.UNSOLD_QY_SALESPROFIT_Y_4)/sum(a.UNSOLD_QY_SALE_INCOME_Y_4)*100	
else sum(a.UNSOLD_SALESPROFIT_Y_4)/sum(a.UNSOLD_SALE_INCOME_Y_4)*100	 end  as UNSOLD_SALESPROFIT_RATE_Y_4	-- 拿地年份T-4年未售销售净利润率
  ,sum(case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then a.UNSOLD_QY_COST_Y/10000 else a.UNSOLD_COST_Y/10000 end)  					as UNSOLD_COST_Y                -- 本年未售成本
  ,case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then  sum(a.UNSOLD_QY_SALESPROFIT_Y)/sum(a.UNSOLD_QY_COST_Y)*100 
  else sum(a.UNSOLD_SALESPROFIT_Y)/sum(a.UNSOLD_COST_Y)*100 end as 	UNSOLD_COSTPROFIT_RATE_Y	-- 本年未售成本利润率
  ,case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then (case when a.M_YEARMONTH=substring(add_months(current_date,-1),1,7) then sum(a.UNSOLD_QY_SALESPROFIT_Y)/sum(a.UNSOLD_QY_COST_Y) else 0 end)*100 -
	(case when a.M_YEARMONTH=substring(add_months(current_date,-13),1,7) then sum(a.UNSOLD_QY_SALESPROFIT_Y)/sum(a.UNSOLD_QY_COST_Y) else 0 end)*100	
else (case when a.M_YEARMONTH=substring(add_months(current_date,-1),1,7) then sum(a.UNSOLD_SALESPROFIT_Y)/sum(a.UNSOLD_COST_Y) else 0 end)*100 -
	(case when a.M_YEARMONTH=substring(add_months(current_date,-13),1,7) then sum(a.UNSOLD_SALESPROFIT_Y)/sum(a.UNSOLD_COST_Y) else 0 end)*100 	end	 as   UNSOLD_COSTPROFIT_RATE_Y2Y	-- 本年未售成本利润率同比
  ,sum(case when a.glkj  in ('全口径-权益','管理口径-权益','并表口径-权益') then a.UNSOLD_QY_TOTAL_Y/10000 else a.UNSOLD_TOTAL_Y/10000 end)  	as UNSOLD_TOTAL_Y                			-- 本年未售总货值
  , DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss') ETL_IN_DT                 -- 记录首次插入表中的时间（系统日期） 
  , 'DWS_CMSK.HS02_SALES_PROFIT_M_REPORT_ST_CS_FINAL_2_SS' DATA_SOURCE                 -- 数据来源 
  , DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss') UPDATE_TIME               							-- 数据更新时间  
  , '' UPDATE_USER_ID            									-- 数据更新人工号  
  , '' UPDATE_USER_NAME          									-- 数据更新人姓名
 ,a.p_day
from ads_cmsk_bap.HA02_MARKET_PROFIT_ANALYSE_MID_MOBIE_2_CS a
group by    
	a.GROUP_NAME                 
   ,a.AREA_GUID                
   ,a.COMPANY_GUID           
   ,a.PROJECT_GUID 
	,a.M_YEARMONTH  -- 年月
  ,a.M_YEAR       -- 年份
  ,a.M_MONTH      -- 月份
  ,a.glkj		  -- 管理口径
  ,a.p_day
GROUPING sets ( (acct, txn_date), acct, txn_date, ( ) )
)

insert OVERWRITE table ads_cmsk_bap.HA02_MARKET_PROFIT_ANALYSE_MOBIE_2_CS 
 select 
a.ORG_TYPE	-- 组织类型（总部、区域、城市公司、项目、分期）
,a.ORG_GUID	 -- 组织编码（总部GUID、区域GUID、城市公司GUID、项目GUID、分期GUID）
,a.M_YEARMONTH	-- 年月
,a.M_YEAR	-- 年份
,a.M_MONTH	-- 月份
,a.MANAG_CALIBER	-- 管理口径
,a.SOLD_SALESPROFIT_Y	-- 本年已售销售净利润
,a.SOLD_SALE_INCOME_Y	-- 本年已售累计销售收入
,case when a.MANAG_CALIBER='管理口径' then b.YEAR_SALE_PROFILE_TARGET
  when a.MANAG_CALIBER='管理口径-权益' then b.YEAR_QY_SALE_PROFILE_TARGET else 0 end--本年已售销售净利润目标
,0	--本年已售累计销售收入目标
,a.SOLD_SALESPROFIT_RATE_Y			--本年已售销售净利率
,case when a.MANAG_CALIBER='管理口径' then b.YEAR_SALE_PROFILE_TARGET/b.YEAR_SALE_TARGET*1.09*100
  when a.MANAG_CALIBER='管理口径-权益' then b.YEAR_QY_SALE_PROFILE_RATE_TARGET*100 else 0 end --本年已售销售净利率目标
,a.SOLD_SALESPROFIT_Y2Y				--本年已售销售净利润同比
,a.SOLD_SALESPROFIT_RATE_Y2Y		--本年已售销售净利率同比
,case when a.MANAG_CALIBER='管理口径' then a.SOLD_SALESPROFIT_Y/b.YEAR_SALE_PROFILE_TARGET*100
  when a.MANAG_CALIBER='管理口径-权益' then a.SOLD_SALESPROFIT_Y/b.YEAR_QY_SALE_PROFILE_TARGET*100 else 0 end		-- 本年已售销售净利润完成率
,case when a.MANAG_CALIBER='管理口径' then a.SOLD_SALESPROFIT_RATE_Y-b.YEAR_SALE_PROFILE_TARGET/b.YEAR_SALE_TARGET*1.09	
  when a.MANAG_CALIBER='管理口径-权益' then a.SOLD_SALESPROFIT_RATE_Y- b.YEAR_QY_SALE_PROFILE_RATE_TARGET else 0 end		-- 本年已售销售净利率偏差pct
,case when a.MANAG_CALIBER='管理口径' and b.YEAR_SALE_PROFILE_TARGET <>0 and b.YEAR_SALE_PROFILE_TARGET is not null then b.YEAR_SALE_PROFILE_TARGET-a.SOLD_SALESPROFIT_Y
  when a.MANAG_CALIBER='管理口径-权益' and b.YEAR_QY_SALE_PROFILE_TARGET <>0 and b.YEAR_QY_SALE_PROFILE_TARGET is not null then b.YEAR_QY_SALE_PROFILE_TARGET-a.SOLD_SALESPROFIT_Y else 0 end		-- 本年已售销售净利润缺口
,a.SOLD_AMOUNT_Y				-- 本年已售金额
,a.SOLD_COST_Y					-- 本年已售成本
,a.SOLD_COSTPROFIT_RATE_Y		-- 本年已售成本利润率
,a.SOLD_COSTPROFIT_RATE_Y2Y		-- 本年已售成本利润率同比
,a.SOLD_SALESPROFIT_M			-- 本月已售销售净利润
,a.SOLD_SALESPROFIT_RATE_M		-- 本月已售销售净利率
,a.SOLD_COSTPROFIT_RATE_M		-- 本月已售成本利润率
,a.SOLD_SALESPROFIT_M_1			-- T-1月已售销售净利润
,a.SOLD_SALESPROFIT_RATE_M_1	-- T-1月已售销售净利率
,a.SOLD_COSTPROFIT_RATE_M_1		-- T-1月已售成本利润率
,a.SOLD_SALESPROFIT_M_2			-- T-2月已售销售净利润
,a.SOLD_SALESPROFIT_RATE_M_2	-- T-2月已售销售净利率
,a.SOLD_COSTPROFIT_RATE_M_2		-- T-2月已售成本利润率
,a.SOLD_SALESPROFIT_M_3			-- T-3月已售销售净利润
,a.SOLD_SALESPROFIT_RATE_M_3	-- T-3月已售销售净利率
,a.SOLD_COSTPROFIT_RATE_M_3		-- T-3月已售成本利润率
,a.SOLD_SALESPROFIT_M_4			-- T-4月已售销售净利润
,a.SOLD_SALESPROFIT_RATE_M_4	-- T-4月已售销售净利率
,a.SOLD_COSTPROFIT_RATE_M_4		-- T-4月已售成本利润率
,a.SOLD_SALESPROFIT_M_5			-- T-5月已售销售净利润
,a.SOLD_SALESPROFIT_RATE_M_5	-- T-5月已售销售净利率
,a.SOLD_COSTPROFIT_RATE_M_5		-- T-5月已售成本利润率
,a.UNSOLD_SALESPROFIT_Y			-- 本年未售销售净利润
,a.UNSOLD_SALE_INCOME_Y			-- 本年未售累计销售收入
,a.UNSOLD_SALESPROFIT_RATE_Y	-- 本年未售销售净利润率
,a.UNSOLD_SALESPROFIT_Y2Y		-- 本年未售销售净利润同比
,a.UNSOLD_SALESPROFIT_RATE_Y2Y	-- 本年未售销售净利润率同比
,a.UNSOLD_SALESPROFIT_Y_0		-- 拿地年份T-0年未售销售净利润
,a.UNSOLD_SALESPROFIT_RATE_Y_0	-- 拿地年份T-0年未售销售净利润率
,a.UNSOLD_SALESPROFIT_Y_1		-- 拿地年份T-1年未售销售净利润
,a.UNSOLD_SALESPROFIT_RATE_Y_1	-- 拿地年份T-1年未售销售净利润率
,a.UNSOLD_SALESPROFIT_Y_2		-- 拿地年份T-2年未售销售净利润
,a.UNSOLD_SALESPROFIT_RATE_Y_2	-- 拿地年份T-2年未售销售净利润率
,a.UNSOLD_SALESPROFIT_Y_3		-- 拿地年份T-3年未售销售净利润
,a.UNSOLD_SALESPROFIT_RATE_Y_3	-- 拿地年份T-3年未售销售净利润率
,a.UNSOLD_SALESPROFIT_Y_4		-- 拿地年份T-4年前未售销售净利润
,a.UNSOLD_SALESPROFIT_RATE_Y_4	-- 拿地年份T-4年前未售销售净利润率
,a.UNSOLD_COST_Y				-- 本年未售成本
,a.UNSOLD_COSTPROFIT_RATE_Y		-- 本年未售成本利润率
,a.UNSOLD_COSTPROFIT_RATE_Y2Y	-- 本年未售成本利润率同比
,a.UNSOLD_TOTAL_Y				-- 本年未售总货值 
, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss') ETL_IN_DT                 -- 记录首次插入表中的时间（系统日期） 
, 'DWS_CMSK.HS02_SALES_PROFIT_M_REPORT_ST_CS_FINAL_2_SS' DATA_SOURCE                 -- 数据来源 
, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss') UPDATE_TIME               							-- 数据更新时间  
, '' UPDATE_USER_ID            									-- 数据更新人工号  
, '' UPDATE_USER_NAME          									-- 数据更新人姓名
from TMP_HA02_MARKET_PROFIT_ANALYSE_MOBIE_ALL a
  left join 
 ( select 
CASE  WHEN  AREA_GUID is null AND COMPANY_GUID is null   THEN '11B11DB4-E907-4F1F-8835-B9DAAB6E1F23'
        	 WHEN  AREA_GUID IS NOT NULL AND COMPANY_GUID is null AND COMPANY_NAME is null  THEN AREA_GUID
	         WHEN  AREA_GUID IS NOT NULL AND COMPANY_GUID IS NOT NULL    THEN COMPANY_GUID	
			 ELSE '存在错误数据' END     AS ORG_GUID 
,ORG_TYPE 
,M_YEAR
,YEAR_SALE_TARGET					-- 签约额
,YEAR_QY_SALE_TARGET				-- 权益签约额
,YEAR_SALE_PROFILE_TARGET			-- 销售净利润
,YEAR_QY_SALE_PROFILE_TARGET		-- 权益销售净利润
,YEAR_QY_SALE_PROFILE_RATE_TARGET	-- 权益销售净利率
from  META.HS02_PROFIT_TARGET_SS 
where p_day=substring(current_date,1,7) and DATA_TYPE='利润') b
  on a.ORG_TYPE=b.ORG_TYPE
  and a.ORG_GUID=b.ORG_GUID
  and a.M_YEAR=b.M_YEAR
  and a.MANAG_CALIBER in ('管理口径','管理口径-权益');

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