package com.github.melin.sqlflow.metadata;

import com.github.melin.sqlflow.tree.QualifiedName;

import java.util.Optional;

/**
 * huaixin 2021/12/22 10:05 AM
 */
public interface Metadata {

    Optional<String> getSchema();

    Optional<String> getCatalog();

    /**
     * Is the named function an aggregation function?  This does not need type parameters
     * because overloads between aggregation and other function types are not allowed.
     */
    boolean isAggregationFunction(QualifiedName name);

    Optional<SchemaTable> getTableSchema(QualifiedObjectName targetTable);

    Optional<ViewDefinition> getView(QualifiedObjectName viewName);
}
