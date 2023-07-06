package io.github.melin.sqlflow.tree.window.rowPattern;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.tree.Node;
import io.github.melin.sqlflow.tree.NodeLocation;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/19 10:14 PM
 */
public class PatternPermutation extends RowPattern {
    private final List<RowPattern> patterns;

    public PatternPermutation(NodeLocation location, List<RowPattern> patterns)
    {
        this(Optional.of(location), patterns);
    }

    private PatternPermutation(Optional<NodeLocation> location, List<RowPattern> patterns)
    {
        super(location);
        this.patterns = requireNonNull(patterns, "patterns is null");
        checkArgument(!patterns.isEmpty(), "patterns list is empty");
    }

    public List<RowPattern> getPatterns()
    {
        return patterns;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitPatternPermutation(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.copyOf(patterns);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        PatternPermutation o = (PatternPermutation) obj;
        return Objects.equals(patterns, o.patterns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(patterns);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("patterns", patterns)
                .toString();
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return Node.sameClass(this, other);
    }
}
