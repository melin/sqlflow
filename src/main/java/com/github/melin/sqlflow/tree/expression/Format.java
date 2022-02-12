package com.github.melin.sqlflow.tree.expression;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.Node;
import com.github.melin.sqlflow.tree.NodeLocation;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 10:39 AM
 */
public class Format extends Expression {
    private final List<Expression> arguments;

    public Format(List<Expression> arguments) {
        this(Optional.empty(), arguments);
    }

    public Format(NodeLocation location, List<Expression> arguments) {
        this(Optional.of(location), arguments);
    }

    private Format(Optional<NodeLocation> location, List<Expression> arguments) {
        super(location);
        requireNonNull(arguments, "arguments is null");
        checkArgument(arguments.size() >= 2, "must have at least two arguments");
        this.arguments = ImmutableList.copyOf(arguments);
    }

    public List<Expression> getArguments() {
        return arguments;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitFormat(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return arguments;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Format o = (Format) obj;
        return Objects.equals(arguments, o.arguments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(arguments);
    }

    @Override
    public boolean shallowEquals(Node other) {
        return sameClass(this, other);
    }
}
