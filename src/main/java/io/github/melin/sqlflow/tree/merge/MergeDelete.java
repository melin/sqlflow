package io.github.melin.sqlflow.tree.merge;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.tree.expression.Identifier;
import io.github.melin.sqlflow.tree.Node;
import io.github.melin.sqlflow.tree.NodeLocation;
import io.github.melin.sqlflow.tree.expression.Expression;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * huaixin 2021/12/21 12:55 PM
 */
public class MergeDelete
        extends MergeCase {
    public MergeDelete(Optional<Expression> expression) {
        this(Optional.empty(), expression);
    }

    public MergeDelete(NodeLocation location, Optional<Expression> expression) {
        super(Optional.of(location), expression);
    }

    public MergeDelete(Optional<NodeLocation> location, Optional<Expression> expression) {
        super(location, expression);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitMergeDelete(this, context);
    }

    @Override
    public List<Identifier> getSetColumns() {
        return ImmutableList.of();
    }

    @Override
    public List<Expression> getSetExpressions() {
        return ImmutableList.of();
    }

    @Override
    public List<? extends Node> getChildren() {
        return expression.map(ImmutableList::of).orElseGet(ImmutableList::of);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expression);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        MergeDelete mergeDelete = (MergeDelete) obj;
        return Objects.equals(expression, mergeDelete.expression);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("expression", expression.orElse(null))
                .omitNullValues()
                .toString();
    }
}
