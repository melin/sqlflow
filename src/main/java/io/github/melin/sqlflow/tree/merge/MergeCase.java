package io.github.melin.sqlflow.tree.merge;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.tree.expression.Identifier;
import io.github.melin.sqlflow.tree.Node;
import io.github.melin.sqlflow.tree.NodeLocation;
import io.github.melin.sqlflow.tree.expression.Expression;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 12:52 PM
 */
public abstract class MergeCase extends Node {
    protected final Optional<Expression> expression;

    protected MergeCase(Optional<NodeLocation> location, Optional<Expression> expression) {
        super(location);
        this.expression = requireNonNull(expression, "expression is null");
    }

    public Optional<Expression> getExpression() {
        return expression;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitMergeCase(this, context);
    }

    public abstract List<Identifier> getSetColumns();

    public abstract List<Expression> getSetExpressions();
}
