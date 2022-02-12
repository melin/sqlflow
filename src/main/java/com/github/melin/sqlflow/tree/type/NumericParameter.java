package com.github.melin.sqlflow.tree.type;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.Node;
import com.github.melin.sqlflow.tree.NodeLocation;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 11:32 AM
 */
public class NumericParameter extends DataTypeParameter {
    private final String value;

    public NumericParameter(NodeLocation location, String value) {
        this(Optional.of(location), value);
    }

    public NumericParameter(Optional<NodeLocation> location, String value) {
        super(location);
        this.value = requireNonNull(value, "value is null");
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitNumericTypeParameter(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NumericParameter that = (NumericParameter) o;
        return value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public boolean shallowEquals(Node other) {
        if (!Node.sameClass(this, other)) {
            return false;
        }

        return Objects.equals(value, ((NumericParameter) other).value);
    }
}
