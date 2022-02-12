package com.github.melin.sqlflow.tree.expression;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.Node;
import com.github.melin.sqlflow.tree.NodeLocation;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 11:26 AM
 */
public class InListExpression extends Expression {
    private final List<Expression> values;

    public InListExpression(List<Expression> values) {
        this(Optional.empty(), values);
    }

    public InListExpression(NodeLocation location, List<Expression> values) {
        this(Optional.of(location), values);
    }

    private InListExpression(Optional<NodeLocation> location, List<Expression> values) {
        super(location);
        requireNonNull(values, "values is null");
        checkArgument(!values.isEmpty(), "values cannot be empty");
        this.values = ImmutableList.copyOf(values);
    }

    public List<Expression> getValues() {
        return values;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitInListExpression(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return values;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InListExpression that = (InListExpression) o;
        return Objects.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return values.hashCode();
    }

    @Override
    public boolean shallowEquals(Node other) {
        return sameClass(this, other);
    }
}
