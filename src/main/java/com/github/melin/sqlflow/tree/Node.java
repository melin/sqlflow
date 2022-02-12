package com.github.melin.sqlflow.tree;

import com.github.melin.sqlflow.AstVisitor;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/18 9:52 PM
 */
public abstract class Node {
    private final Optional<NodeLocation> location;

    protected Node(Optional<NodeLocation> location) {
        this.location = requireNonNull(location, "location is null");
    }

    /**
     * Accessible for {@link AstVisitor}, use {@link AstVisitor#process(Node, Object)} instead.
     */
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitNode(this, context);
    }

    public Optional<NodeLocation> getLocation() {
        return location;
    }

    public abstract List<? extends Node> getChildren();

    // Force subclasses to have a proper equals and hashcode implementation
    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object obj);

    @Override
    public abstract String toString();

    /**
     * Compare with another node by considering internal state excluding any Node returned by getChildren()
     */
    public boolean shallowEquals(Node other) {
        throw new UnsupportedOperationException("not yet implemented: " + getClass().getName());
    }

    public static boolean sameClass(Node left, Node right) {
        if (left == right) {
            return true;
        }

        return left.getClass() == right.getClass();
    }
}

