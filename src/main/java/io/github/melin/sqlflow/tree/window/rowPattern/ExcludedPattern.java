package io.github.melin.sqlflow.tree.window.rowPattern;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.tree.Node;
import io.github.melin.sqlflow.tree.NodeLocation;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/19 10:12 PM
 */
public class ExcludedPattern extends RowPattern {

    private final RowPattern pattern;

    public ExcludedPattern(NodeLocation location, RowPattern pattern) {
        this(Optional.of(location), pattern);
    }

    private ExcludedPattern(Optional<NodeLocation> location, RowPattern pattern) {
        super(location);
        this.pattern = requireNonNull(pattern, "pattern is null");
    }

    public RowPattern getPattern() {
        return pattern;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitExcludedPattern(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(pattern);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        ExcludedPattern o = (ExcludedPattern) obj;
        return Objects.equals(pattern, o.pattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pattern);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("pattern", pattern)
                .toString();
    }

    @Override
    public boolean shallowEquals(Node other) {
        return sameClass(this, other);
    }
}
