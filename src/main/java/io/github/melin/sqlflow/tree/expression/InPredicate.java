package io.github.melin.sqlflow.tree.expression;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.tree.Node;
import io.github.melin.sqlflow.tree.NodeLocation;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * huaixin 2021/12/21 11:07 AM
 */
public class InPredicate extends Expression {
    private final Expression value;
    private final Expression valueList;

    public InPredicate(Expression value, Expression valueList) {
        this(Optional.empty(), value, valueList);
    }

    public InPredicate(NodeLocation location, Expression value, Expression valueList) {
        this(Optional.of(location), value, valueList);
    }

    private InPredicate(Optional<NodeLocation> location, Expression value, Expression valueList) {
        super(location);
        this.value = value;
        this.valueList = valueList;
    }

    public Expression getValue() {
        return value;
    }

    public Expression getValueList() {
        return valueList;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitInPredicate(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(value, valueList);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InPredicate that = (InPredicate) o;
        return Objects.equals(value, that.value) &&
                Objects.equals(valueList, that.valueList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, valueList);
    }

    @Override
    public boolean shallowEquals(Node other) {
        return Node.sameClass(this, other);
    }
}
