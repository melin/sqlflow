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
 * huaixin 2021/12/21 10:50 AM
 */
public class SubscriptExpression extends Expression {
    private final Expression base;
    private final Expression index;

    public SubscriptExpression(Expression base, Expression index) {
        this(Optional.empty(), base, index);
    }

    public SubscriptExpression(NodeLocation location, Expression base, Expression index) {
        this(Optional.of(location), base, index);
    }

    private SubscriptExpression(Optional<NodeLocation> location, Expression base, Expression index) {
        super(location);
        this.base = requireNonNull(base, "base is null");
        this.index = requireNonNull(index, "index is null");
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSubscriptExpression(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(base, index);
    }

    public Expression getBase() {
        return base;
    }

    public Expression getIndex() {
        return index;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SubscriptExpression that = (SubscriptExpression) o;

        return Objects.equals(this.base, that.base) && Objects.equals(this.index, that.index);
    }

    @Override
    public int hashCode() {
        return Objects.hash(base, index);
    }

    @Override
    public boolean shallowEquals(Node other) {
        return sameClass(this, other);
    }
}
