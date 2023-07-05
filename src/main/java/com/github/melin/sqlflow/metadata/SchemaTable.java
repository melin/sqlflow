package com.github.melin.sqlflow.metadata;

import java.util.List;

import static com.google.common.collect.MoreCollectors.toOptional;

public final class SchemaTable {

    private final String catalogName;

    private final String schemaName;

    private final String tableName;

    private List<String> columns;

    public SchemaTable(String catalogName, String schemaName, String tableName, List<String> columns) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columns = columns;
    }

    public SchemaTable(String schemaName, String tableName, List<String> columns) {
        this.catalogName = null;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columns = columns;
    }

    public SchemaTable(String tableName, List<String> columns) {
        this.catalogName = null;
        this.schemaName = null;
        this.tableName = tableName;
        this.columns = columns;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public String getColumn(String name) {
        return columns.stream()
                .filter(column -> column.equals(name))
                .collect(toOptional())
                .orElseThrow(() -> new IllegalArgumentException("Invalid column name: " + name));
    }

    public String toString() {
        if (catalogName != null) {
            return catalogName + '.' + schemaName + '.' + tableName;
        } else if (schemaName != null) {
            return schemaName + '.' + tableName;
        } else {
            return tableName;
        }
    }
}
