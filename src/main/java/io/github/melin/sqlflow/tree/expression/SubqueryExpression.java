package io.github.melin.sqlflow.tree.expression;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.tree.Node;
import io.github.melin.sqlflow.tree.NodeLocation;
import io.github.melin.sqlflow.tree.statement.Query;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * huaixin 2021/12/21 10:51 AM
 */
public class SubqueryExpression
        extends Expression {
    private final Query query;

    public SubqueryExpression(Query query) {
        this(Optional.empty(), query);
    }

    public SubqueryExpression(NodeLocation location, Query query) {
        this(Optional.of(location), query);
    }

    private SubqueryExpression(Optional<NodeLocation> location, Query query) {
        super(location);
        this.query = query;
    }

    public Query getQuery() {
        return query;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSubqueryExpression(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(query);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SubqueryExpression that = (SubqueryExpression) o;
        return Objects.equals(query, that.query);
    }

    @Override
    public int hashCode() {
        return query.hashCode();
    }

    @Override
    public boolean shallowEquals(Node other) {
        return sameClass(this, other);
    }
}
