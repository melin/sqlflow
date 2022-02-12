package com.github.melin.sqlflow.tree.statement;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.Node;
import com.github.melin.sqlflow.tree.NodeLocation;

import java.util.Optional;

/**
 * huaixin 2021/12/18 10:03 PM
 */
public abstract class Statement
        extends Node {
    protected Statement(Optional<NodeLocation> location) {
        super(location);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitStatement(this, context);
    }
}
