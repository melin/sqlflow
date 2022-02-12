package com.github.melin.sqlflow.tree.literal;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.expression.Expression;
import com.github.melin.sqlflow.tree.Node;
import com.github.melin.sqlflow.tree.NodeLocation;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * huaixin 2021/12/19 9:18 PM
 */
public abstract class Literal
        extends Expression {
    protected Literal(Optional<NodeLocation> location) {
        super(location);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitLiteral(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }
}

