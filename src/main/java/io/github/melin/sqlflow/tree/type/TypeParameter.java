package io.github.melin.sqlflow.tree.type;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.tree.Node;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 11:35 AM
 */
public class TypeParameter extends DataTypeParameter {
    private final DataType type;

    public TypeParameter(DataType type) {
        super(Optional.empty());
        this.type = requireNonNull(type, "type is null");
    }

    public DataType getValue() {
        return type;
    }

    @Override
    public String toString() {
        return type.toString();
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(type);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitTypeParameter(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TypeParameter that = (TypeParameter) o;
        return type.equals(that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type);
    }

    @Override
    public boolean shallowEquals(Node other) {
        return sameClass(this, other);
    }
}
