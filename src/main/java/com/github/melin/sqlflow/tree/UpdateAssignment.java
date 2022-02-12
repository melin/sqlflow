package com.github.melin.sqlflow.tree;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.expression.Expression;
import com.github.melin.sqlflow.tree.expression.Identifier;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 12:58 PM
 */
public class UpdateAssignment extends Node {
    private final Identifier name;
    private final Expression value;

    public UpdateAssignment(Identifier name, Expression value) {
        this(Optional.empty(), name, value);
    }

    public UpdateAssignment(NodeLocation location, Identifier name, Expression value) {
        this(Optional.of(location), name, value);
    }

    private UpdateAssignment(Optional<NodeLocation> location, Identifier name, Expression value) {
        super(location);
        this.name = requireNonNull(name, "name is null");
        this.value = requireNonNull(value, "value is null");
    }

    public Identifier getName() {
        return name;
    }

    public Expression getValue() {
        return value;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitUpdateAssignment(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(name, value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        UpdateAssignment other = (UpdateAssignment) obj;
        return Objects.equals(name, other.name) &&
                Objects.equals(value, other.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value);
    }

    @Override
    public String toString() {
        return format("%s = %s", name, value);
    }
}
