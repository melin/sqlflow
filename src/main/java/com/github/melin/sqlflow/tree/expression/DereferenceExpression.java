package com.github.melin.sqlflow.tree.expression;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.Node;
import com.github.melin.sqlflow.tree.NodeLocation;
import com.github.melin.sqlflow.tree.QualifiedName;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 10:58 AM
 */
public class DereferenceExpression
        extends Expression {
    private final Expression base;
    private final Optional<Identifier> field;

    public DereferenceExpression(Expression base, Identifier field) {
        this(Optional.empty(), base, Optional.of(field));
    }

    public DereferenceExpression(NodeLocation location, Expression base, Identifier field) {
        this(Optional.of(location), base, Optional.of(field));
    }

    public DereferenceExpression(Identifier label) {
        this(Optional.empty(), label, Optional.empty());
    }

    public DereferenceExpression(NodeLocation location, Identifier label) {
        this(Optional.of(location), label, Optional.empty());
    }

    private DereferenceExpression(Optional<NodeLocation> location, Expression base, Optional<Identifier> field) {
        super(location);
        checkArgument(base != null, "base is null");
        requireNonNull(field, "field is null");
        this.base = base;
        this.field = field;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDereferenceExpression(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> children = ImmutableList.builder();
        children.add(base);
        field.ifPresent(children::add);
        return children.build();
    }

    public Expression getBase() {
        return base;
    }

    public Optional<Identifier> getField() {
        return field;
    }

    /**
     * If this DereferenceExpression looks like a QualifiedName, return QualifiedName.
     * Otherwise return null
     */
    public static QualifiedName getQualifiedName(DereferenceExpression expression) {
        if (!expression.field.isPresent()) {
            return null;
        }

        Identifier field = expression.field.get();

        List<Identifier> parts = null;
        if (expression.base instanceof Identifier) {
            parts = ImmutableList.of((Identifier) expression.base, field);
        } else if (expression.base instanceof DereferenceExpression) {
            QualifiedName baseQualifiedName = getQualifiedName((DereferenceExpression) expression.base);
            if (baseQualifiedName != null) {
                ImmutableList.Builder<Identifier> builder = ImmutableList.builder();
                builder.addAll(baseQualifiedName.getOriginalParts());
                builder.add(field);
                parts = builder.build();
            }
        }

        return parts == null ? null : QualifiedName.of(parts);
    }

    public static Expression from(QualifiedName name) {
        Expression result = null;

        for (String part : name.getParts()) {
            if (result == null) {
                result = new Identifier(part);
            } else {
                result = new DereferenceExpression(result, new Identifier(part));
            }
        }

        return result;
    }

    public static boolean isQualifiedAllFieldsReference(Expression expression) {
        return expression instanceof DereferenceExpression && !((DereferenceExpression) expression).field.isPresent();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DereferenceExpression that = (DereferenceExpression) o;
        return Objects.equals(base, that.base) &&
                Objects.equals(field, that.field);
    }

    @Override
    public int hashCode() {
        return Objects.hash(base, field);
    }

    @Override
    public boolean shallowEquals(Node other) {
        return sameClass(this, other);
    }
}
