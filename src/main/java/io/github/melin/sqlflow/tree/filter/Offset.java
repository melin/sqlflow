package io.github.melin.sqlflow.tree.filter;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.tree.expression.Expression;
import io.github.melin.sqlflow.tree.Node;
import io.github.melin.sqlflow.tree.NodeLocation;
import io.github.melin.sqlflow.tree.literal.LongLiteral;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * huaixin 2021/12/19 9:35 PM
 */
public class Offset extends Node {
    private final Expression rowCount;

    public Offset(Expression rowCount) {
        this(Optional.empty(), rowCount);
    }

    public Offset(NodeLocation location, Expression rowCount) {
        this(Optional.of(location), rowCount);
    }

    public Offset(Optional<NodeLocation> location, Expression rowCount) {
        super(location);
        checkArgument(rowCount instanceof LongLiteral,
                "unexpected rowCount class: %s",
                rowCount.getClass().getSimpleName());
        this.rowCount = rowCount;
    }

    public Expression getRowCount() {
        return rowCount;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitOffset(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(rowCount);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        Offset o = (Offset) obj;
        return Objects.equals(rowCount, o.rowCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowCount);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("rowCount", rowCount)
                .toString();
    }

    @Override
    public boolean shallowEquals(Node other) {
        return sameClass(this, other);
    }
}
