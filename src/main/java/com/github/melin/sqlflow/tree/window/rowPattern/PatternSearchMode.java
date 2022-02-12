package com.github.melin.sqlflow.tree.window.rowPattern;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.Node;
import com.github.melin.sqlflow.tree.NodeLocation;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/19 9:52 PM
 */
public final class PatternSearchMode extends Node {
    private final Mode mode;

    public PatternSearchMode(Mode mode) {
        this(Optional.empty(), mode);
    }

    public PatternSearchMode(NodeLocation location, Mode mode) {
        this(Optional.of(location), mode);
    }

    public PatternSearchMode(Optional<NodeLocation> location, Mode mode) {
        super(location);
        this.mode = requireNonNull(mode, "mode is null");
    }

    public Mode getMode() {
        return mode;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitPatternSearchMode(this, context);
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
        return mode == ((PatternSearchMode) obj).mode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(mode);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("mode", mode)
                .toString();
    }

    @Override
    public boolean shallowEquals(Node other) {
        if (!sameClass(this, other)) {
            return false;
        }

        return mode == ((PatternSearchMode) other).mode;
    }

    public enum Mode {
        INITIAL, SEEK
    }
}
