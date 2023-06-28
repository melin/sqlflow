package com.github.melin.sqlflow;

import com.github.melin.sqlflow.metadata.MetadataService;
import com.github.melin.sqlflow.metadata.QualifiedObjectName;
import com.github.melin.sqlflow.metadata.SchemaTable;
import com.github.melin.sqlflow.metadata.ViewDefinition;
import com.github.melin.sqlflow.tree.QualifiedName;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * huaixin 2021/12/25 6:13 PM
 */
public class SimpleMetadataService implements MetadataService {

    private final List<SchemaTable> tables = new ArrayList<>();

    private final Optional<String> defaultSchema;

    private final Optional<String> defaultDatabase;

    public SimpleMetadataService(Optional<String> defaultSchema, Optional<String> defaultDatabase) {
        this.defaultSchema = defaultSchema;
        this.defaultDatabase = defaultDatabase;
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
        return defaultDatabase;
    }

    @Override
    public boolean isAggregationFunction(QualifiedName name) {
        return false;
    }

    @Override
    public Optional<SchemaTable> getTableSchema(QualifiedObjectName table) {
        for (SchemaTable schemaTable : tables) {
            if (schemaTable.toString().equals(table.toString())) {
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
