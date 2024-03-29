package io.github.melin.sqlflow.tree.group;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.tree.expression.Expression;
import io.github.melin.sqlflow.tree.Node;
import io.github.melin.sqlflow.tree.NodeLocation;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 10:04 AM
 */
public final class GroupingSets extends GroupingElement {
    private final List<List<Expression>> sets;

    public GroupingSets(List<List<Expression>> groupingSets) {
        this(Optional.empty(), groupingSets);
    }

    public GroupingSets(NodeLocation location, List<List<Expression>> sets) {
        this(Optional.of(location), sets);
    }

    private GroupingSets(Optional<NodeLocation> location, List<List<Expression>> sets) {
        super(location);
        requireNonNull(sets, "sets is null");
        checkArgument(!sets.isEmpty(), "grouping sets cannot be empty");
        this.sets = sets.stream().map(ImmutableList::copyOf).collect(toImmutableList());
    }

    public List<List<Expression>> getSets() {
        return sets;
    }

    @Override
    public List<Expression> getExpressions() {
        return sets.stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitGroupingSets(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GroupingSets groupingSets = (GroupingSets) o;
        return Objects.equals(sets, groupingSets.sets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sets);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("sets", sets)
                .toString();
    }

    @Override
    public boolean shallowEquals(Node other) {
        if (!Node.sameClass(this, other)) {
            return false;
        }

        GroupingSets that = (GroupingSets) other;
        return Objects.equals(sets, that.sets);
    }
}
