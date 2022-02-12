package com.github.melin.sqlflow.tree.window;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.expression.Expression;
import com.github.melin.sqlflow.tree.expression.Identifier;
import com.github.melin.sqlflow.tree.Node;
import com.github.melin.sqlflow.tree.NodeLocation;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/19 9:54 PM
 */
public class VariableDefinition extends Node {
    private final Identifier name;
    private final Expression expression;

    public VariableDefinition(Identifier name, Expression expression) {
        this(Optional.empty(), name, expression);
    }

    public VariableDefinition(NodeLocation location, Identifier name, Expression expression) {
        this(Optional.of(location), name, expression);
    }

    private VariableDefinition(Optional<NodeLocation> location, Identifier name, Expression expression) {
        super(location);
        this.name = requireNonNull(name, "name is null");
        this.expression = requireNonNull(expression, "expression is null");
    }

    public Identifier getName() {
        return name;
    }

    public Expression getExpression() {
        return expression;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitVariableDefinition(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(expression);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("name", name)
                .add("expression", expression)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        VariableDefinition that = (VariableDefinition) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(expression, that.expression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, expression);
    }

    @Override
    public boolean shallowEquals(Node other) {
        if (!sameClass(this, other)) {
            return false;
        }

        return Objects.equals(name, ((VariableDefinition) other).name);
    }
}
