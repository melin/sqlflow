package com.github.melin.sqlflow.tree.window.rowPattern;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.NodeLocation;

import java.util.Optional;

/**
 * huaixin 2021/12/19 10:16 PM
 */
public class ZeroOrOneQuantifier extends PatternQuantifier {
    public ZeroOrOneQuantifier(boolean greedy) {
        this(Optional.empty(), greedy);
    }

    public ZeroOrOneQuantifier(NodeLocation location, boolean greedy) {
        this(Optional.of(location), greedy);
    }

    public ZeroOrOneQuantifier(Optional<NodeLocation> location, boolean greedy) {
        super(location, greedy);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitZeroOrOneQuantifier(this, context);
    }
}
