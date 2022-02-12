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
 * huaixin 2021/12/21 11:05 AM
 */
public class SimpleCaseExpression
        extends Expression {
    private final Expression operand;
    private final List<WhenClause> whenClauses;
    private final Optional<Expression> defaultValue;

    public SimpleCaseExpression(Expression operand, List<WhenClause> whenClauses, Optional<Expression> defaultValue) {
        this(Optional.empty(), operand, whenClauses, defaultValue);
    }

    public SimpleCaseExpression(NodeLocation location, Expression operand, List<WhenClause> whenClauses, Optional<Expression> defaultValue) {
        this(Optional.of(location), operand, whenClauses, defaultValue);
    }

    private SimpleCaseExpression(Optional<NodeLocation> location, Expression operand, List<WhenClause> whenClauses, Optional<Expression> defaultValue) {
        super(location);
        requireNonNull(operand, "operand is null");
        requireNonNull(whenClauses, "whenClauses is null");

        this.operand = operand;
        this.whenClauses = ImmutableList.copyOf(whenClauses);
        this.defaultValue = defaultValue;
    }

    public Expression getOperand() {
        return operand;
    }

    public List<WhenClause> getWhenClauses() {
        return whenClauses;
    }

    public Optional<Expression> getDefaultValue() {
        return defaultValue;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSimpleCaseExpression(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.add(operand);
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

        SimpleCaseExpression that = (SimpleCaseExpression) o;
        return Objects.equals(operand, that.operand) &&
                Objects.equals(whenClauses, that.whenClauses) &&
                Objects.equals(defaultValue, that.defaultValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operand, whenClauses, defaultValue);
    }

    @Override
    public boolean shallowEquals(Node other) {
        return Node.sameClass(this, other);
    }
}
