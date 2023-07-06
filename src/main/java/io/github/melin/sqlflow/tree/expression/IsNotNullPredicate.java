package io.github.melin.sqlflow.tree.expression;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.tree.Node;
import io.github.melin.sqlflow.tree.NodeLocation;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 11:23 AM
 */
public class IsNotNullPredicate extends Expression {
    private final Expression value;

    public IsNotNullPredicate(Expression value) {
        this(Optional.empty(), value);
    }

    public IsNotNullPredicate(NodeLocation location, Expression value) {
        this(Optional.of(location), value);
    }

    private IsNotNullPredicate(Optional<NodeLocation> location, Expression value) {
        super(location);
        requireNonNull(value, "value is null");
        this.value = value;
    }

    public Expression getValue() {
        return value;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitIsNotNullPredicate(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IsNotNullPredicate that = (IsNotNullPredicate) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean shallowEquals(Node other) {
        return Node.sameClass(this, other);
    }
}
