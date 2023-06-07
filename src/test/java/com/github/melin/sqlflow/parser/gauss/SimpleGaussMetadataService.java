package com.github.melin.sqlflow.parser.gauss;

import com.github.melin.sqlflow.metadata.*;
import com.github.melin.sqlflow.tree.QualifiedName;
import com.github.melin.sqlflow.type.BigintType;
import com.github.melin.sqlflow.type.VarcharType;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;

/**
 * huaixin 2021/12/25 6:13 PM
 */
public class SimpleGaussMetadataService implements MetadataService {
    @Override
    public Optional<String> getSchema() {
        return Optional.of("bigdata");
    }

    @Override
    public Optional<String> getCatalog() {
        return Optional.of("default");
    }

    @Override
    public boolean isAggregationFunction(QualifiedName name) {
        return false;
    }

    @Override
    public Optional<SchemaTable> getTableSchema(QualifiedObjectName table) {
        if (table.getObjectName().equals("test")) {
            List<ColumnSchema> columns = Lists.newArrayList();
            columns.add(new ColumnSchema("col1", VarcharType.VARCHAR, false));
            columns.add(new ColumnSchema("col2", VarcharType.VARCHAR, false));
            columns.add(new ColumnSchema("type", VarcharType.VARCHAR, false));
            columns.add(new ColumnSchema("row_num", BigintType.BIGINT, false));
            columns.add(new ColumnSchema("ds", VarcharType.VARCHAR, false));

            return Optional.of(new SchemaTable("test", columns));
        } else if (table.getObjectName().equals("demo")) {
            List<ColumnSchema> columns = Lists.newArrayList();
            columns.add(new ColumnSchema("name", VarcharType.VARCHAR, false));
            columns.add(new ColumnSchema("row_num", BigintType.BIGINT, false));

            return Optional.of(new SchemaTable("demo", columns));
        }
        return Optional.empty();
    }

    @Override
    public Optional<ViewDefinition> getView(QualifiedObjectName viewName) {
        return Optional.empty();
    }
}
