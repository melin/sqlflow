package com.github.melin.sqlflow.tree.filter;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.expression.Expression;
import com.github.melin.sqlflow.tree.Node;
import com.github.melin.sqlflow.tree.NodeLocation;
import com.github.melin.sqlflow.tree.literal.LongLiteral;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * huaixin 2021/12/19 9:38 PM
 */
public class FetchFirst extends Node {
    private final Optional<Expression> rowCount;
    private final boolean withTies;

    public FetchFirst(Expression rowCount) {
        this(Optional.empty(), Optional.of(rowCount), false);
    }

    public FetchFirst(Expression rowCount, boolean withTies) {
        this(Optional.empty(), Optional.of(rowCount), withTies);
    }

    public FetchFirst(Optional<Expression> rowCount) {
        this(Optional.empty(), rowCount, false);
    }

    public FetchFirst(Optional<Expression> rowCount, boolean withTies) {
        this(Optional.empty(), rowCount, withTies);
    }

    public FetchFirst(Optional<NodeLocation> location, Optional<Expression> rowCount, boolean withTies) {
        super(location);
        rowCount.ifPresent(count -> checkArgument(
                count instanceof LongLiteral,
                "unexpected rowCount class: %s",
                rowCount.getClass().getSimpleName()));
        this.rowCount = rowCount;
        this.withTies = withTies;
    }

    public Optional<Expression> getRowCount() {
        return rowCount;
    }

    public boolean isWithTies() {
        return withTies;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitFetchFirst(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return rowCount.map(ImmutableList::of).orElse(ImmutableList.of());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if ((o == null) || (getClass() != o.getClass())) {
            return false;
        }
        FetchFirst that = (FetchFirst) o;
        return withTies == that.withTies &&
                Objects.equals(rowCount, that.rowCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowCount, withTies);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("rowCount", rowCount.orElse(null))
                .add("withTies", withTies)
                .omitNullValues()
                .toString();
    }

    @Override
    public boolean shallowEquals(Node other) {
        if (!sameClass(this, other)) {
            return false;
        }

        FetchFirst otherNode = (FetchFirst) other;

        return withTies == otherNode.withTies;
    }
}
