package io.github.melin.sqlflow.tree.window;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.tree.expression.Expression;
import io.github.melin.sqlflow.tree.expression.Identifier;
import io.github.melin.sqlflow.tree.Node;
import io.github.melin.sqlflow.tree.NodeLocation;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/19 9:49 PM
 */
public class MeasureDefinition extends Node {
    private final Expression expression;
    private final Identifier name;

    public MeasureDefinition(Expression expression, Identifier name)
    {
        this(Optional.empty(), expression, name);
    }

    public MeasureDefinition(NodeLocation location, Expression expression, Identifier name)
    {
        this(Optional.of(location), expression, name);
    }

    private MeasureDefinition(Optional<NodeLocation> location, Expression expression, Identifier name)
    {
        super(location);
        this.expression = requireNonNull(expression, "expression is null");
        this.name = requireNonNull(name, "name is null");
    }

    public Expression getExpression()
    {
        return expression;
    }

    public Identifier getName()
    {
        return name;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitMeasureDefinition(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of(expression);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("expression", expression)
                .add("name", name)
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MeasureDefinition that = (MeasureDefinition) o;
        return Objects.equals(expression, that.expression) &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(expression, name);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        return Objects.equals(name, ((MeasureDefinition) other).name);
    }
}
