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
 * huaixin 2021/12/19 10:11 PM
 */
public class AnchorPattern extends RowPattern {
    public enum Type {
        PARTITION_START,
        PARTITION_END
    }

    private final Type type;

    public AnchorPattern(NodeLocation location, Type type) {
        this(Optional.of(location), type);
    }

    private AnchorPattern(Optional<NodeLocation> location, Type type) {
        super(location);
        this.type = requireNonNull(type, "type is null");
    }

    public Type getType() {
        return type;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAnchorPattern(this, context);
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
        AnchorPattern o = (AnchorPattern) obj;
        return Objects.equals(type, o.type);
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("type", type)
                .toString();
    }

    @Override
    public boolean shallowEquals(Node other) {
        return Node.sameClass(this, other);
    }
}
