package com.github.melin.sqlflow.tree.type;

import com.github.melin.sqlflow.tree.NodeLocation;
import com.github.melin.sqlflow.tree.expression.Expression;

import java.util.Optional;

/**
 * huaixin 2021/12/21 10:55 AM
 */
public abstract class DataType extends Expression {
    public DataType(Optional<NodeLocation> location) {
        super(location);
    }
}
