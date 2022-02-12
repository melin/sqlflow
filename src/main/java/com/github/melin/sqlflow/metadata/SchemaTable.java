package com.github.melin.sqlflow.metadata;

import java.util.List;

import static com.google.common.collect.MoreCollectors.toOptional;

public final class SchemaTable {
    private final String tableName;

    private List<ColumnSchema> columns;

    public SchemaTable(String tableName, List<ColumnSchema> columns) {
        this.tableName = tableName;
        this.columns = columns;
    }

    public String getTableName() {
        return tableName;
    }

    public List<ColumnSchema> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnSchema> columns) {
        this.columns = columns;
    }

    public ColumnSchema getColumn(String name) {
        return columns.stream()
                .filter(columnMetadata -> columnMetadata.getName().equals(name))
                .collect(toOptional())
                .orElseThrow(() -> new IllegalArgumentException("Invalid column name: " + name));
    }
}
