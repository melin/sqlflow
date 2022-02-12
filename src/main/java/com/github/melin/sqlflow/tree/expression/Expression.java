package com.github.melin.sqlflow.tree.expression;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.formatter.ExpressionFormatter;
import com.github.melin.sqlflow.tree.Node;
import com.github.melin.sqlflow.tree.NodeLocation;

import java.util.Optional;

/**
 * huaixin 2021/12/18 9:54 PM
 */
public abstract class Expression
        extends Node {
    public Expression(Optional<NodeLocation> location) {
        super(location);
    }

    /**
     * Accessible for {@link AstVisitor}, use {@link AstVisitor#process(Node, Object)} instead.
     */
    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitExpression(this, context);
    }

    @Override
    public final String toString() {
        return ExpressionFormatter.formatExpression(this);
    }
}
