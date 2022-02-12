package com.github.melin.sqlflow;

import com.github.melin.sqlflow.tree.expression.SubqueryExpression;

/**
 * When walking Expressions, don't traverse into SubqueryExpressions
 */
public abstract class DefaultExpressionTraversalVisitor<C>
        extends DefaultTraversalVisitor<C> {
    @Override
    public Void visitSubqueryExpression(SubqueryExpression node, C context) {
        // Don't traverse into Subqueries within an Expression
        return null;
    }
}
