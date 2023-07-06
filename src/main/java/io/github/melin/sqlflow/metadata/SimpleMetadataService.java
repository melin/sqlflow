package io.github.melin.sqlflow.metadata;

import io.github.melin.sqlflow.tree.QualifiedName;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * huaixin 2021/12/25 6:13 PM
 */
public class SimpleMetadataService implements MetadataService {

    private final List<SchemaTable> tables = new ArrayList<>();

    private final Optional<String> defaultSchema;

    private final Optional<String> defaultCatalog;

    public SimpleMetadataService(String schema) {
        this.defaultCatalog = Optional.empty();
        this.defaultSchema = Optional.of(schema);
    }

    public SimpleMetadataService(String defaultCatalog, String defaultSchema) {
        this.defaultCatalog = Optional.of(defaultCatalog);
        this.defaultSchema = Optional.of(defaultSchema);
    }

    public MetadataService addTableMetadata(List<SchemaTable> schemaTables) {
        tables.addAll(schemaTables);
        return this;
    }

    public MetadataService addTableMetadata(SchemaTable schemaTable) {
        tables.add(schemaTable);
        return this;
    }

    @Override
    public Optional<String> getSchema() {
        return defaultSchema;
    }

    @Override
    public Optional<String> getCatalog() {
        return defaultCatalog;
    }

    @Override
    public boolean isAggregationFunction(QualifiedName name) {
        return false;
    }

    @Override
    public Optional<SchemaTable> getTableSchema(QualifiedObjectName table) {
        for (SchemaTable schemaTable : tables) {
            if (schemaTable.toString().equalsIgnoreCase(table.toString())) {
                return Optional.of(schemaTable);
            }
        }

        return Optional.empty();
    }

    @Override
    public Optional<ViewDefinition> getView(QualifiedObjectName viewName) {
        return Optional.empty();
    }
}
