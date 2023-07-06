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
 * huaixin 2021/12/21 11:25 AM
 */
public class BetweenPredicate
        extends Expression {
    private final Expression value;
    private final Expression min;
    private final Expression max;

    public BetweenPredicate(Expression value, Expression min, Expression max) {
        this(Optional.empty(), value, min, max);
    }

    public BetweenPredicate(NodeLocation location, Expression value, Expression min, Expression max) {
        this(Optional.of(location), value, min, max);
    }

    private BetweenPredicate(Optional<NodeLocation> location, Expression value, Expression min, Expression max) {
        super(location);
        requireNonNull(value, "value is null");
        requireNonNull(min, "min is null");
        requireNonNull(max, "max is null");

        this.value = value;
        this.min = min;
        this.max = max;
    }

    public Expression getValue() {
        return value;
    }

    public Expression getMin() {
        return min;
    }

    public Expression getMax() {
        return max;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitBetweenPredicate(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(value, min, max);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BetweenPredicate that = (BetweenPredicate) o;
        return Objects.equals(value, that.value) &&
                Objects.equals(min, that.min) &&
                Objects.equals(max, that.max);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, min, max);
    }

    @Override
    public boolean shallowEquals(Node other) {
        return sameClass(this, other);
    }
}
