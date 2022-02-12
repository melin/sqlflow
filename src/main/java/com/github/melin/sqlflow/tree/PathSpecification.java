package com.github.melin.sqlflow.tree;

import com.github.melin.sqlflow.AstVisitor;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 1:31 PM
 */
public final class PathSpecification extends Node {
    private List<PathElement> path;

    public PathSpecification(NodeLocation location, List<PathElement> path) {
        this(Optional.of(location), path);
    }

    @VisibleForTesting
    public PathSpecification(Optional<NodeLocation> location, List<PathElement> path) {
        super(location);
        this.path = ImmutableList.copyOf(requireNonNull(path, "path is null"));
    }

    public List<PathElement> getPath() {
        return path;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitPathSpecification(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return path;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        PathSpecification o = (PathSpecification) obj;
        return Objects.equals(path, o.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path);
    }

    @Override
    public String toString() {
        return Joiner.on(", ").join(path);
    }
}
