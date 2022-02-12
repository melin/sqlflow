package com.github.melin.sqlflow.tree.relation;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.Node;
import com.github.melin.sqlflow.tree.NodeLocation;

import java.util.Optional;

/**
 * huaixin 2021/12/18 10:06 PM
 */
public abstract class Relation extends Node {
    public Relation(Optional<NodeLocation> location) {
        super(location);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitRelation(this, context);
    }
}

