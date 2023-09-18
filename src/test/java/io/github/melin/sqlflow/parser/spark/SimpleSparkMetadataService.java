package io.github.melin.sqlflow.parser.spark;

import io.github.melin.sqlflow.metadata.MetadataService;
import io.github.melin.sqlflow.metadata.QualifiedObjectName;
import io.github.melin.sqlflow.metadata.SchemaTable;
import io.github.melin.sqlflow.metadata.ViewDefinition;
import io.github.melin.sqlflow.tree.QualifiedName;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;

/**
 * huaixin 2021/12/25 6:13 PM
 */
public class SimpleSparkMetadataService implements MetadataService {
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
        if (table.getObjectName().equalsIgnoreCase("test")) {
            List<String> columns = Lists.newArrayList();
            columns.add("col1");
            columns.add("col2");
            columns.add("type");
            columns.add("row_num");
            columns.add("ds");

            return Optional.of(new SchemaTable("test", columns));
        } else if (table.getObjectName().equalsIgnoreCase("demo")) {
            List<String> columns = Lists.newArrayList();
            columns.add("name");
            columns.add("row_num");

            return Optional.of(new SchemaTable("demo", columns));
        }
        return Optional.empty();
    }

    @Override
    public Optional<ViewDefinition> getView(QualifiedObjectName viewName) {
        return Optional.empty();
    }
}
