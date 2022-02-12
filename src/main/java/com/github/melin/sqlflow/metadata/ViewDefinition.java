package com.github.melin.sqlflow.metadata;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/25 5:35 PM
 */
public class ViewDefinition {
    private final String originalSql;
    private final Optional<String> catalog;
    private final Optional<String> schema;
    private final List<ViewColumn> columns;
    private final Optional<String> comment;

    public ViewDefinition(
            String originalSql,
            Optional<String> catalog,
            Optional<String> schema,
            List<ViewColumn> columns,
            Optional<String> comment) {
        this.originalSql = requireNonNull(originalSql, "originalSql is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.columns = Lists.newArrayList(requireNonNull(columns, "columns is null"));
        this.comment = requireNonNull(comment, "comment is null");
        checkArgument(schema == null || catalog.isPresent(), "catalog must be present if schema is present");
        checkArgument(!columns.isEmpty(), "columns list is empty");
    }

    public String getOriginalSql() {
        return originalSql;
    }

    public Optional<String> getCatalog() {
        return catalog;
    }

    public Optional<String> getSchema() {
        return schema;
    }

    public List<ViewColumn> getColumns() {
        return columns;
    }

    public Optional<String> getComment() {
        return comment;
    }

    @Override
    public String toString() {
        return toStringHelper(this).omitNullValues()
                .add("originalSql", originalSql)
                .add("catalog", catalog.orElse(null))
                .add("schema", schema.orElse(null))
                .add("columns", columns)
                .add("comment", comment.orElse(null))
                .toString();
    }
}
