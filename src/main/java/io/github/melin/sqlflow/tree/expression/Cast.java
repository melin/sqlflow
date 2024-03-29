package io.github.melin.sqlflow.tree.expression;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.tree.Node;
import io.github.melin.sqlflow.tree.NodeLocation;
import io.github.melin.sqlflow.tree.type.DataType;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 10:53 AM
 */
public final class Cast extends Expression {
    private final Expression expression;
    private final DataType type;
    private final boolean safe;
    private final boolean typeOnly;

    public Cast(Expression expression, DataType type) {
        this(Optional.empty(), expression, type, false, false);
    }

    public Cast(Expression expression, DataType type, boolean safe) {
        this(Optional.empty(), expression, type, safe, false);
    }

    public Cast(Expression expression, DataType type, boolean safe, boolean typeOnly) {
        this(Optional.empty(), expression, type, safe, typeOnly);
    }

    public Cast(NodeLocation location, Expression expression, DataType type) {
        this(Optional.of(location), expression, type, false, false);
    }

    public Cast(NodeLocation location, Expression expression, DataType type, boolean safe) {
        this(Optional.of(location), expression, type, safe, false);
    }

    private Cast(Optional<NodeLocation> location, Expression expression, DataType type, boolean safe, boolean typeOnly) {
        super(location);
        requireNonNull(expression, "expression is null");

        this.expression = expression;
        this.type = type;
        this.safe = safe;
        this.typeOnly = typeOnly;
    }

    public Expression getExpression() {
        return expression;
    }

    public DataType getType() {
        return type;
    }

    public boolean isSafe() {
        return safe;
    }

    public boolean isTypeOnly() {
        return typeOnly;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCast(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(expression, type);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Cast cast = (Cast) o;
        return safe == cast.safe &&
                typeOnly == cast.typeOnly &&
                expression.equals(cast.expression) &&
                type.equals(cast.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expression, type, safe, typeOnly);
    }

    @Override
    public boolean shallowEquals(Node other) {
        if (!sameClass(this, other)) {
            return false;
        }

        Cast otherCast = (Cast) other;
        return safe == otherCast.safe &&
                typeOnly == otherCast.typeOnly;
    }
}
