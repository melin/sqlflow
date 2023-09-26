package io.github.melin.sqlflow.tree.statement;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.tree.*;
import io.github.melin.sqlflow.tree.NodeLocation;
import io.github.melin.sqlflow.tree.QualifiedName;
import io.github.melin.sqlflow.tree.expression.Identifier;
import io.github.melin.sqlflow.tree.relation.Table;
import io.github.melin.sqlflow.tree.Node;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/18 10:22 PM
 */
public final class Insert extends Statement {
    private final Optional<With> with;
    private final Table table;
    private final Query query;
    private final Optional<List<Identifier>> columns;

    public Insert(Optional<With> with, Table table, Optional<List<Identifier>> columns, Query query) {
        this(Optional.empty(), with, table, columns, query);
    }

    private Insert(Optional<NodeLocation> location, Optional<With> with, Table table, Optional<List<Identifier>> columns, Query query) {
        super(location);
        this.with = with;
        this.table = requireNonNull(table, "target is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.query = requireNonNull(query, "query is null");
    }

    public Table getTable() {
        return table;
    }

    public QualifiedName getTarget() {
        return table.getName();
    }

    public Optional<List<Identifier>> getColumns() {
        return columns;
    }

    public Query getQuery() {
        return query;
    }

    public Optional<With> getWith() {
        return with;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitInsert(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(table, columns, query);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        Insert o = (Insert) obj;
        return Objects.equals(table, o.table) && Objects.equals(columns, o.columns) && Objects.equals(query, o.query);
    }

    @Override
    public String toString() {
        return toStringHelper(this).add("table", table).add("columns", columns).add("query", query).toString();
    }
}
