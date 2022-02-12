package com.github.melin.sqlflow.tree.expression;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.Node;
import com.github.melin.sqlflow.tree.NodeLocation;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 11:08 AM
 */
public class SearchedCaseExpression
        extends Expression {
    private final List<WhenClause> whenClauses;
    private final Optional<Expression> defaultValue;

    public SearchedCaseExpression(List<WhenClause> whenClauses, Optional<Expression> defaultValue) {
        this(Optional.empty(), whenClauses, defaultValue);
    }

    public SearchedCaseExpression(NodeLocation location, List<WhenClause> whenClauses, Optional<Expression> defaultValue) {
        this(Optional.of(location), whenClauses, defaultValue);
    }

    private SearchedCaseExpression(Optional<NodeLocation> location, List<WhenClause> whenClauses, Optional<Expression> defaultValue) {
        super(location);
        requireNonNull(whenClauses, "whenClauses is null");
        requireNonNull(defaultValue, "defaultValue is null");
        this.whenClauses = ImmutableList.copyOf(whenClauses);
        this.defaultValue = defaultValue;
    }

    public List<WhenClause> getWhenClauses() {
        return whenClauses;
    }

    public Optional<Expression> getDefaultValue() {
        return defaultValue;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSearchedCaseExpression(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.addAll(whenClauses);
        defaultValue.ifPresent(nodes::add);
        return nodes.build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SearchedCaseExpression that = (SearchedCaseExpression) o;
        return Objects.equals(whenClauses, that.whenClauses) &&
                Objects.equals(defaultValue, that.defaultValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(whenClauses, defaultValue);
    }

    @Override
    public boolean shallowEquals(Node other) {
        return sameClass(this, other);
    }
}
