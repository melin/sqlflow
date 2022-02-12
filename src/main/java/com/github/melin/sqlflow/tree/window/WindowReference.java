package com.github.melin.sqlflow.tree.window;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.expression.Identifier;
import com.github.melin.sqlflow.tree.Node;
import com.github.melin.sqlflow.tree.NodeLocation;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/19 9:48 PM
 */
public class WindowReference extends Node implements Window {
    private final Identifier name;

    public WindowReference(Identifier name) {
        this(Optional.empty(), name);
    }

    public WindowReference(NodeLocation location, Identifier name) {
        this(Optional.of(location), name);
    }

    private WindowReference(Optional<NodeLocation> location, Identifier name) {
        super(location);
        this.name = requireNonNull(name, "name is null");
    }

    public Identifier getName() {
        return name;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitWindowReference(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(name);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        WindowReference o = (WindowReference) obj;
        return Objects.equals(name, o.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("name", name)
                .toString();
    }

    @Override
    public boolean shallowEquals(Node other) {
        return sameClass(this, other);
    }
}
