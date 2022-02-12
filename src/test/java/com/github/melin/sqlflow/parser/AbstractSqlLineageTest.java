package com.github.melin.sqlflow.parser;

import com.github.melin.sqlflow.analyzer.Analysis;
import com.github.melin.sqlflow.analyzer.OutputColumn;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * huaixin 2021/12/26 9:06 PM
 */
public abstract class AbstractSqlLineageTest {

    protected void assertLineage(Analysis analysis, OutputColumn... outputColumns)
            throws Exception {

        if (outputColumns.length != 0) {
            assertThat(analysis.getTarget().get().getColumns().get())
                    .containsExactly(outputColumns);
        }
    }
}
