package io.github.melin.sqlflow.tree.type;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.tree.Node;
import io.github.melin.sqlflow.tree.NodeLocation;

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
