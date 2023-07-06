package io.github.melin.sqlflow.tree.window.rowPattern;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.tree.Node;
import io.github.melin.sqlflow.tree.NodeLocation;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * huaixin 2021/12/19 10:09 PM
 */
public abstract class PatternQuantifier extends Node {
    private final boolean greedy;

    protected PatternQuantifier(Optional<NodeLocation> location, boolean greedy) {
        super(location);
        this.greedy = greedy;
    }

    public boolean isGreedy() {
        return greedy;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitPatternQuantifier(this, context);
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
        PatternQuantifier o = (PatternQuantifier) obj;
        return greedy == o.greedy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(greedy);
    }

    @Override
    public boolean shallowEquals(Node other) {
        if (!sameClass(this, other)) {
            return false;
        }

        PatternQuantifier otherNode = (PatternQuantifier) other;
        return greedy == otherNode.greedy;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("greedy", greedy)
                .toString();
    }
}
