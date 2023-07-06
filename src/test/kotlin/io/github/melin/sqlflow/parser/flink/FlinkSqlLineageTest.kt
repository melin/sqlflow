package io.github.melin.sqlflow.parser.flink

import io.github.melin.sqlflow.analyzer.Analysis
import io.github.melin.sqlflow.analyzer.StatementAnalyzer
import io.github.melin.sqlflow.parser.SqlParser
import io.github.melin.sqlflow.util.JsonUtils
import org.junit.Test
import java.util.*

class FlinkSqlLineageTest {

    protected val SQL_PARSER = SqlParser()

    @Test
    @Throws(Exception::class)
    fun testInsertInto() {
        val sql = """
            INSERT INTO PROCESSED_MDM_PRODUCT_ENRICHMENT(PRODUCT_ID, ENRICHMENT_ID, LANG, ENRICHMENT_VALUE,LAST_UPDATED)
            SELECT
            	b.ITEM PRODUCT_ID,
            	CAST(CAST(b.UDA_ID AS DECIMAL(5, 0)) AS STRING) ENRICHMENT_ID,
            	c.ISO_CODE LANG,
            	b.TRANSLATED_VALUE ENRICHMENT_VALUE,
            	b.LAST_UPDATE_DATETIME LAST_UPDATED
            FROM
            	RETEK_XX_ITEM_ATTR_TRANSLATE_PRODUCT_ENRICHMENT b
            JOIN MDM_DIM_LANG_LOOKUPMAP_ORACLE FOR SYSTEM_TIME AS OF b.KAFKA_PROCESS_TIME AS c
                  ON
            	CAST(b.LANG AS DECIMAL(6, 0)) = c.LANG
            JOIN MDM_DIM_UDA_ITEM_FF_LOOKUPMAP_ORACLE FOR SYSTEM_TIME AS OF b.KAFKA_PROCESS_TIME AS uif
                    ON
            	uif.ITEM = b.ITEM
            	AND CAST(b.UDA_ID AS DECIMAL(5, 0)) = uif.UDA_ID
            JOIN MDM_DIM_PRODUCT_ATTRIB_TYPE_LOOKUPMAP_MYSQL FOR SYSTEM_TIME AS OF b.KAFKA_PROCESS_TIME AS pat
                  ON
            	CAST(CAST(b.UDA_ID AS DECIMAL(5, 0)) AS STRING) = pat.ATTRIB_ID
            	AND pat.BU_CODE = 'WTCTH'
            	AND pat.ATTRIB_TYPE = 'PRODUCT_ENRICHMENT'
            UNION ALL
            SELECT
            	'WTCTH' BU_CODE,
            	uif.ITEM PRODUCT_ID,
            	CAST(CAST(uif.UDA_ID AS DECIMAL(5, 0)) AS STRING) ENRICHMENT_ID,
            	c.ISO_CODE LANG,
            	b.TRANSLATED_VALUE ENRICHMENT_VALUE,
            	uif.LAST_UPDATE_DATETIME LAST_UPDATED
            FROM
            	RETEK_UDA_ITEM_FF_PRODUCT_ENRICHMENT_DIM uif
            JOIN MDM_DIM_XX_ITEM_ATTR_TRANSLATE_LOOKUPMAP_ORACLE_DIM FOR SYSTEM_TIME AS OF uif.KAFKA_PROCESS_TIME AS b
                    ON
            	uif.ITEM = b.ITEM
            	AND CAST(uif.UDA_ID AS DECIMAL(5, 0)) = b.UDA_ID
            JOIN MDM_DIM_LANG_LOOKUPMAP_ORACLE FOR SYSTEM_TIME AS OF uif.KAFKA_PROCESS_TIME AS c
                  ON
            	CAST(b.LANG AS DECIMAL(6, 0)) = c.LANG
            JOIN MDM_DIM_PRODUCT_ATTRIB_TYPE_LOOKUPMAP_MYSQL FOR SYSTEM_TIME AS OF uif.KAFKA_PROCESS_TIME AS pat
                  ON
            	CAST(CAST(uif.UDA_ID AS DECIMAL(5, 0)) AS STRING) = pat.ATTRIB_ID
            	AND pat.ATTRIB_TYPE = 'PRODUCT_ENRICHMENT'
        """.trimIndent()
        val statement = SQL_PARSER.createStatement(sql)
        val analysis = Analysis(statement, emptyMap())
        val statementAnalyzer = StatementAnalyzer(
            analysis,
            SimpleFlinkMetadataService(), SQL_PARSER
        )
        statementAnalyzer.analyze(statement, Optional.empty())

        System.out.println(JsonUtils.toJSONString(analysis.getTarget().get()));
    }
}