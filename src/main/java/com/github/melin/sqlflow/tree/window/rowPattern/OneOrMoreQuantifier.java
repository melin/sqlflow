package com.github.melin.sqlflow.tree.window.rowPattern;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.NodeLocation;

import java.util.Optional;

/**
 * huaixin 2021/12/19 10:17 PM
 */
public class OneOrMoreQuantifier extends PatternQuantifier {
    public OneOrMoreQuantifier(boolean greedy) {
        this(Optional.empty(), greedy);
    }

    public OneOrMoreQuantifier(NodeLocation location, boolean greedy) {
        this(Optional.of(location), greedy);
    }

    public OneOrMoreQuantifier(Optional<NodeLocation> location, boolean greedy) {
        super(location, greedy);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitOneOrMoreQuantifier(this, context);
    }
}
