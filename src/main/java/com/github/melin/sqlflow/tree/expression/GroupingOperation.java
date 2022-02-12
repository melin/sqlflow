package com.github.melin.sqlflow.tree.expression;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.Node;
import com.github.melin.sqlflow.tree.NodeLocation;
import com.github.melin.sqlflow.tree.QualifiedName;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 10:57 AM
 */
public class GroupingOperation
        extends Expression {
    private final List<Expression> groupingColumns;

    public GroupingOperation(Optional<NodeLocation> location, List<QualifiedName> groupingColumns) {
        super(location);
        requireNonNull(groupingColumns);
        checkArgument(!groupingColumns.isEmpty(), "grouping operation columns cannot be empty");
        this.groupingColumns = groupingColumns.stream()
                .map(DereferenceExpression::from)
                .collect(toImmutableList());
    }

    public List<Expression> getGroupingColumns() {
        return groupingColumns;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitGroupingOperation(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return groupingColumns;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GroupingOperation other = (GroupingOperation) o;
        return Objects.equals(groupingColumns, other.groupingColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupingColumns);
    }

    @Override
    public boolean shallowEquals(Node other) {
        return sameClass(this, other);
    }
}
