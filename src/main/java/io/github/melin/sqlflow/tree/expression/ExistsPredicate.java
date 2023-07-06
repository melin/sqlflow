package io.github.melin.sqlflow.tree.expression;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.tree.Node;
import io.github.melin.sqlflow.tree.NodeLocation;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 10:52 AM
 */
public class ExistsPredicate extends Expression {
    private final Expression subquery;

    public ExistsPredicate(Expression subquery) {
        this(Optional.empty(), subquery);
    }

    public ExistsPredicate(NodeLocation location, Expression subquery) {
        this(Optional.of(location), subquery);
    }

    private ExistsPredicate(Optional<NodeLocation> location, Expression subquery) {
        super(location);
        requireNonNull(subquery, "subquery is null");
        this.subquery = subquery;
    }

    public Expression getSubquery() {
        return subquery;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitExists(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(subquery);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ExistsPredicate that = (ExistsPredicate) o;
        return Objects.equals(subquery, that.subquery);
    }

    @Override
    public int hashCode() {
        return subquery.hashCode();
    }

    @Override
    public boolean shallowEquals(Node other) {
        return Node.sameClass(this, other);
    }
}
