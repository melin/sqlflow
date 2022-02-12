package com.github.melin.sqlflow.tree;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.expression.Identifier;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 1:30 PM
 */
public final class PathElement extends Node {
    private final Optional<Identifier> catalog;
    private final Identifier schema;

    public PathElement(NodeLocation location, Identifier schema) {
        this(Optional.of(location), Optional.empty(), schema);
    }

    @VisibleForTesting
    public PathElement(Optional<Identifier> catalog, Identifier schema) {
        this(Optional.empty(), catalog, schema);
    }

    public PathElement(NodeLocation location, Identifier catalog, Identifier schema) {
        this(Optional.of(location), Optional.of(catalog), schema);
    }

    private PathElement(Optional<NodeLocation> location, Optional<Identifier> catalog, Identifier schema) {
        super(location);
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
    }

    public Optional<Identifier> getCatalog() {
        return catalog;
    }

    public Identifier getSchema() {
        return schema;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitPathElement(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        PathElement o = (PathElement) obj;
        return Objects.equals(schema, o.schema) &&
                Objects.equals(catalog, o.catalog);
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalog, schema);
    }

    @Override
    public String toString() {
        if (catalog.isPresent()) {
            return format("%s.%s", catalog.get(), schema);
        }
        return schema.toString();
    }
}
