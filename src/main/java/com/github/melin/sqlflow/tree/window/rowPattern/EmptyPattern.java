package com.github.melin.sqlflow.tree.window.rowPattern;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.Node;
import com.github.melin.sqlflow.tree.NodeLocation;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * huaixin 2021/12/19 10:11 PM
 */
public class EmptyPattern extends RowPattern {
    public EmptyPattern(NodeLocation location) {
        this(Optional.of(location));
    }

    private EmptyPattern(Optional<NodeLocation> location) {
        super(location);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitEmptyPattern(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .toString();
    }

    @Override
    public boolean shallowEquals(Node other) {
        return sameClass(this, other);
    }
}
