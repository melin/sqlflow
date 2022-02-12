package com.github.melin.sqlflow.tree.relation;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.Node;
import com.github.melin.sqlflow.tree.NodeLocation;
import com.github.melin.sqlflow.tree.QualifiedName;
import com.github.melin.sqlflow.tree.QueryPeriod;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * huaixin 2021/12/18 10:16 PM
 */
public class Table extends QueryBody {
    private final QualifiedName name;
    private final Optional<QueryPeriod> queryPeriod;

    public Table(QualifiedName name) {
        this(Optional.empty(), name, Optional.empty());
    }

    public Table(NodeLocation location, QualifiedName name) {
        this(Optional.of(location), name, Optional.empty());
    }

    public Table(NodeLocation location, QualifiedName name, QueryPeriod queryPeriod) {
        this(Optional.of(location), name, Optional.of(queryPeriod));
    }

    private Table(Optional<NodeLocation> location, QualifiedName name, Optional<QueryPeriod> queryPeriod) {
        super(location);
        this.name = name;
        this.queryPeriod = queryPeriod;
    }

    public QualifiedName getName() {
        return name;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitTable(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        if (queryPeriod.isPresent()) {
            return ImmutableList.of(queryPeriod.get());
        }
        return ImmutableList.of();
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .addValue(name)
                .addValue(queryPeriod)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Table table = (Table) o;
        return Objects.equals(name, table.name) &&
                Objects.equals(queryPeriod, table.getQueryPeriod());
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, queryPeriod);
    }

    @Override
    public boolean shallowEquals(Node other) {
        if (!sameClass(this, other)) {
            return false;
        }

        Table otherTable = (Table) other;
        return name.equals(otherTable.name);
    }

    public Optional<QueryPeriod> getQueryPeriod() {
        return queryPeriod;
    }
}
