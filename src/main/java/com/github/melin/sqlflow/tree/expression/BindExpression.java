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
 * huaixin 2021/12/21 1:22 PM
 */
public class BindExpression extends Expression {
    private final List<Expression> values;
    // Function expression must be of function type.
    // It is not necessarily a lambda. For example, it can be another bind expression.
    private final Expression function;

    public BindExpression(List<Expression> values, Expression function) {
        this(Optional.empty(), values, function);
    }

    public BindExpression(NodeLocation location, List<Expression> values, Expression function) {
        this(Optional.of(location), values, function);
    }

    private BindExpression(Optional<NodeLocation> location, List<Expression> values, Expression function) {
        super(location);
        this.values = requireNonNull(values, "values is null");
        this.function = requireNonNull(function, "function is null");
    }

    public List<Expression> getValues() {
        return values;
    }

    public Expression getFunction() {
        return function;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitBindExpression(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        return nodes.addAll(values)
                .add(function)
                .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BindExpression that = (BindExpression) o;
        return Objects.equals(values, that.values) &&
                Objects.equals(function, that.function);
    }

    @Override
    public int hashCode() {
        return Objects.hash(values, function);
    }

    @Override
    public boolean shallowEquals(Node other) {
        return sameClass(this, other);
    }
}
