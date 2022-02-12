package com.github.melin.sqlflow.tree.group;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.expression.Expression;
import com.github.melin.sqlflow.tree.Node;
import com.github.melin.sqlflow.tree.NodeLocation;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 10:01 AM
 */
public final class SimpleGroupBy extends GroupingElement {
    private final List<Expression> columns;

    public SimpleGroupBy(List<Expression> simpleGroupByExpressions) {
        this(Optional.empty(), simpleGroupByExpressions);
    }

    public SimpleGroupBy(NodeLocation location, List<Expression> simpleGroupByExpressions) {
        this(Optional.of(location), simpleGroupByExpressions);
    }

    private SimpleGroupBy(Optional<NodeLocation> location, List<Expression> simpleGroupByExpressions) {
        super(location);
        this.columns = ImmutableList.copyOf(requireNonNull(simpleGroupByExpressions, "simpleGroupByExpressions is null"));
    }

    @Override
    public List<Expression> getExpressions() {
        return columns;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSimpleGroupBy(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return columns;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SimpleGroupBy that = (SimpleGroupBy) o;
        return Objects.equals(columns, that.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columns);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("columns", columns)
                .toString();
    }

    @Override
    public boolean shallowEquals(Node other) {
        return sameClass(this, other);
    }
}
