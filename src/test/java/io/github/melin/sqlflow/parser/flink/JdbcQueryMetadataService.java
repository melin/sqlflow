package io.github.melin.sqlflow.parser.flink;

import io.github.melin.sqlflow.metadata.MetadataService;
import io.github.melin.sqlflow.metadata.QualifiedObjectName;
import io.github.melin.sqlflow.metadata.SchemaTable;
import io.github.melin.sqlflow.metadata.ViewDefinition;
import io.github.melin.sqlflow.tree.QualifiedName;

import java.util.Optional;

public class JdbcQueryMetadataService implements MetadataService {

    @Override
    public Optional<String> getSchema() {
        return Optional.empty();
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
    public Optional<SchemaTable> getTableSchema(QualifiedObjectName targetTable) {
        return Optional.empty();
    }

    @Override
    public Optional<ViewDefinition> getView(QualifiedObjectName viewName) {
        return Optional.empty();
    }
}
