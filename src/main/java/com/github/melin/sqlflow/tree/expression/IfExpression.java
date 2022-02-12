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
 * huaixin 2021/12/21 11:17 AM
 */
public class IfExpression extends Expression {
    private final Expression condition;
    private final Expression trueValue;
    private final Optional<Expression> falseValue;

    public IfExpression(Expression condition, Expression trueValue, Expression falseValue) {
        this(Optional.empty(), condition, trueValue, falseValue);
    }

    public IfExpression(NodeLocation location, Expression condition, Expression trueValue, Expression falseValue) {
        this(Optional.of(location), condition, trueValue, falseValue);
    }

    private IfExpression(Optional<NodeLocation> location, Expression condition, Expression trueValue, Expression falseValue) {
        super(location);
        this.condition = requireNonNull(condition, "condition is null");
        this.trueValue = requireNonNull(trueValue, "trueValue is null");
        this.falseValue = Optional.ofNullable(falseValue);
    }

    public Expression getCondition() {
        return condition;
    }

    public Expression getTrueValue() {
        return trueValue;
    }

    public Optional<Expression> getFalseValue() {
        return falseValue;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitIfExpression(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        falseValue.ifPresent(nodes::add);
        return nodes.add(condition)
                .add(trueValue)
                .build();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        IfExpression o = (IfExpression) obj;
        return Objects.equals(condition, o.condition) &&
                Objects.equals(trueValue, o.trueValue) &&
                Objects.equals(falseValue, o.falseValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(condition, trueValue, falseValue);
    }

    @Override
    public boolean shallowEquals(Node other) {
        return sameClass(this, other);
    }
}
