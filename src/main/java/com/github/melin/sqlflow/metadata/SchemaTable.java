package com.github.melin.sqlflow.metadata;

import java.util.List;

import static com.google.common.collect.MoreCollectors.toOptional;

public final class SchemaTable {
    private final String tableName;

    private List<String> columns;

    public SchemaTable(String tableName, List<String> columns) {
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
}
