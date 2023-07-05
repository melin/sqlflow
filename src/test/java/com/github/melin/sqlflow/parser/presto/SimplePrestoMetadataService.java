package com.github.melin.sqlflow.parser.presto;

import com.github.melin.sqlflow.metadata.*;
import com.github.melin.sqlflow.tree.QualifiedName;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;

/**
 * huaixin 2021/12/25 6:13 PM
 */
public class SimplePrestoMetadataService implements MetadataService {
    @Override
    public Optional<String> getSchema() {
        return Optional.of("default");
    }

    @Override
    public Optional<String> getCatalog() {
        return Optional.empty();
    }

    @Override
    public boolean isAggregationFunction(QualifiedName name) {
        return false;
    }

    @Override
    public Optional<SchemaTable> getTableSchema(QualifiedObjectName table) {
        if (table.getObjectName().equals("test")) {
            List<String> columns = Lists.newArrayList();
            columns.add("col1");
            columns.add("col2");
            columns.add("type");
            columns.add("row_num");
            columns.add("ds");

            return Optional.of(new SchemaTable("default", "test", columns));
        } else if (table.getObjectName().equals("demo")) {
            List<String> columns = Lists.newArrayList();
            columns.add("name");
            columns.add("row_num");

            return Optional.of(new SchemaTable("default", "demo", columns));
        }
        return Optional.empty();
    }

    @Override
    public Optional<ViewDefinition> getView(QualifiedObjectName viewName) {
        return Optional.empty();
    }
}
