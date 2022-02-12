package com.github.melin.sqlflow.tree.window.rowPattern;

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
 * huaixin 2021/12/19 10:15 PM
 */
public class PatternVariable extends RowPattern {
    private final Identifier name;

    public PatternVariable(NodeLocation location, Identifier name) {
        this(Optional.of(location), name);
    }

    private PatternVariable(Optional<NodeLocation> location, Identifier name) {
        super(location);
        this.name = requireNonNull(name, "name is null");
    }

    public Identifier getName() {
        return name;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitPatternVariable(this, context);
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
        PatternVariable o = (PatternVariable) obj;
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
        return Node.sameClass(this, other);
    }
}
