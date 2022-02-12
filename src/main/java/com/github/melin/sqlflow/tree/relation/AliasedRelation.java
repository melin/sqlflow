package com.github.melin.sqlflow.tree.relation;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.expression.Identifier;
import com.github.melin.sqlflow.tree.Node;
import com.github.melin.sqlflow.tree.NodeLocation;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/19 3:24 PM
 */
public class AliasedRelation
        extends Relation {
    private final Relation relation;
    private final Identifier alias;
    private final List<Identifier> columnNames;

    public AliasedRelation(Relation relation, Identifier alias, List<Identifier> columnNames) {
        this(Optional.empty(), relation, alias, columnNames);
    }

    public AliasedRelation(NodeLocation location, Relation relation, Identifier alias, List<Identifier> columnNames) {
        this(Optional.of(location), relation, alias, columnNames);
    }

    private AliasedRelation(Optional<NodeLocation> location, Relation relation, Identifier alias, List<Identifier> columnNames) {
        super(location);
        requireNonNull(relation, "relation is null");
        requireNonNull(alias, "alias is null");

        this.relation = relation;
        this.alias = alias;
        this.columnNames = columnNames;
    }

    public Relation getRelation() {
        return relation;
    }

    public Identifier getAlias() {
        return alias;
    }

    public List<Identifier> getColumnNames() {
        return columnNames;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAliasedRelation(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(relation);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("relation", relation)
                .add("alias", alias)
                .add("columnNames", columnNames)
                .omitNullValues()
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

        AliasedRelation that = (AliasedRelation) o;
        return Objects.equals(relation, that.relation) &&
                Objects.equals(alias, that.alias) &&
                Objects.equals(columnNames, that.columnNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(relation, alias, columnNames);
    }

    @Override
    public boolean shallowEquals(Node other) {
        if (!sameClass(this, other)) {
            return false;
        }

        AliasedRelation otherRelation = (AliasedRelation) other;
        return alias.equals(otherRelation.alias) && Objects.equals(columnNames, otherRelation.columnNames);
    }
}
