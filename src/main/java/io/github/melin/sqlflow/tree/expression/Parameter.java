package io.github.melin.sqlflow.tree.expression;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.tree.Node;
import io.github.melin.sqlflow.tree.NodeLocation;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * huaixin 2021/12/21 1:02 PM
 */
public class Parameter extends Expression {
    private final int position;

    public Parameter(int id) {
        this(Optional.empty(), id);
    }

    public Parameter(NodeLocation location, int id) {
        this(Optional.of(location), id);
    }

    private Parameter(Optional<NodeLocation> location, int position) {
        super(location);
        this.position = position;
    }

    public int getPosition() {
        return position;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitParameter(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Parameter that = (Parameter) o;
        return Objects.equals(position, that.position);
    }

    @Override
    public int hashCode() {
        return position;
    }

    @Override
    public boolean shallowEquals(Node other) {
        if (!sameClass(this, other)) {
            return false;
        }

        return position == ((Parameter) other).position;
    }
}
