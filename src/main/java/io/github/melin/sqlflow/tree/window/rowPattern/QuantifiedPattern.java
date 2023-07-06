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
 * huaixin 2021/12/19 10:08 PM
 */
public class QuantifiedPattern extends RowPattern {
    private final RowPattern pattern;
    private final PatternQuantifier patternQuantifier;

    public QuantifiedPattern(NodeLocation location, RowPattern pattern, PatternQuantifier patternQuantifier) {
        this(Optional.of(location), pattern, patternQuantifier);
    }

    private QuantifiedPattern(Optional<NodeLocation> location, RowPattern pattern, PatternQuantifier patternQuantifier) {
        super(location);
        this.pattern = requireNonNull(pattern, "pattern is null");
        this.patternQuantifier = requireNonNull(patternQuantifier, "patternQuantifier is null");
    }

    public RowPattern getPattern() {
        return pattern;
    }

    public PatternQuantifier getPatternQuantifier() {
        return patternQuantifier;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitQuantifiedPattern(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(pattern, patternQuantifier);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        QuantifiedPattern o = (QuantifiedPattern) obj;
        return Objects.equals(pattern, o.pattern) &&
                Objects.equals(patternQuantifier, o.patternQuantifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pattern, patternQuantifier);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("pattern", pattern)
                .add("patternQuantifier", patternQuantifier)
                .toString();
    }

    @Override
    public boolean shallowEquals(Node other) {
        return sameClass(this, other);
    }
}
