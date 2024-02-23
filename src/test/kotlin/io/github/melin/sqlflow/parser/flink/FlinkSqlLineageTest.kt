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
            	b.UDA_ID ENRICHMENT_ID,
                CASE 
                    WHEN b.TRANSLATED_VALUE ='406B0386-9A3E-4E5E-890D-A0436EC41DED' THEN '27BD6CBF-4453-4F40-88C3-4C134EF28062'  
                    WHEN b.TRANSLATED_VALUE ='601C3120-09A8-40E5-BD4F-034644486CD8' THEN 'BA19D640-D15D-45F4-8587-B2983A126001' 
                    ELSE b.TRANSLATED_VALUE 
                END,
            	b.TRANSLATED_VALUE ENRICHMENT_VALUE,
            	b.LAST_UPDATE_DATETIME LAST_UPDATED
            FROM
            	RETEK_XX_ITEM_ATTR_TRANSLATE_PRODUCT_ENRICHMENT b
            JOIN MDM_DIM_LANG_LOOKUPMAP_ORACLE AS c
                ON b.LANG = c.LANG
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