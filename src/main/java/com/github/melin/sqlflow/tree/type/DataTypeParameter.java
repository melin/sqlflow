package com.github.melin.sqlflow.tree.type;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.Node;
import com.github.melin.sqlflow.tree.NodeLocation;

import java.util.Optional;

/**
 * huaixin 2021/12/21 11:31 AM
 */
public abstract class DataTypeParameter extends Node {
    protected DataTypeParameter(Optional<NodeLocation> location) {
        super(location);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDataTypeParameter(this, context);
    }
}
