package io.github.melin.sqlflow.tree.group;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.tree.expression.Expression;
import io.github.melin.sqlflow.tree.Node;
import io.github.melin.sqlflow.tree.NodeLocation;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 10:02 AM
 */
public final class Cube extends GroupingElement {
    private final List<Expression> columns;

    public Cube(List<Expression> columns) {
        this(Optional.empty(), columns);
    }

    public Cube(NodeLocation location, List<Expression> columns) {
        this(Optional.of(location), columns);
    }

    private Cube(Optional<NodeLocation> location, List<Expression> columns) {
        super(location);
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
    }

    @Override
    public List<Expression> getExpressions() {
        return columns;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCube(this, context);
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
        Cube cube = (Cube) o;
        return Objects.equals(columns, cube.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columns);
    }

    @Override
    public String toString() {
        return toStringHelper(this).add("columns", columns).toString();
    }
}
