package io.github.melin.sqlflow.tree.group;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.tree.expression.Expression;
import io.github.melin.sqlflow.tree.Node;
import io.github.melin.sqlflow.tree.NodeLocation;

import java.util.List;
import java.util.Optional;

/**
 * huaixin 2021/12/18 11:23 PM
 */
public abstract class GroupingElement
        extends Node {
    protected GroupingElement(Optional<NodeLocation> location) {
        super(location);
    }

    public abstract List<Expression> getExpressions();

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitGroupingElement(this, context);
    }
}
