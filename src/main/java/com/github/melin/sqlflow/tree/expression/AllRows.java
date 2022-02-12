package com.github.melin.sqlflow.tree.expression;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.Node;
import com.github.melin.sqlflow.tree.NodeLocation;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * huaixin 2021/12/21 10:46 AM
 */
public class AllRows
        extends Expression {
    public AllRows() {
        this(Optional.empty());
    }

    public AllRows(NodeLocation location) {
        this(Optional.of(location));
    }

    public AllRows(Optional<NodeLocation> location) {
        super(location);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAllRows(this, context);
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
        return (o != null) && (getClass() == o.getClass());
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    @Override
    public boolean shallowEquals(Node other) {
        return sameClass(this, other);
    }
}
