package com.github.melin.sqlflow.tree;

import com.github.melin.sqlflow.AstVisitor;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/18 10:08 PM
 */
public class OrderBy
        extends Node {
    private final List<SortItem> sortItems;

    public OrderBy(List<SortItem> sortItems) {
        this(Optional.empty(), sortItems);
    }

    public OrderBy(NodeLocation location, List<SortItem> sortItems) {
        this(Optional.of(location), sortItems);
    }

    private OrderBy(Optional<NodeLocation> location, List<SortItem> sortItems) {
        super(location);
        requireNonNull(sortItems, "sortItems is null");
        checkArgument(!sortItems.isEmpty(), "sortItems should not be empty");
        this.sortItems = ImmutableList.copyOf(sortItems);
    }

    public List<SortItem> getSortItems() {
        return sortItems;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitOrderBy(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return sortItems;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("sortItems", sortItems)
                .toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        OrderBy o = (OrderBy) obj;
        return Objects.equals(sortItems, o.sortItems);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sortItems);
    }

    @Override
    public boolean shallowEquals(Node other) {
        return sameClass(this, other);
    }
}
