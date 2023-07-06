package io.github.melin.sqlflow.tree.expression;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.tree.Node;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * huaixin 2021/12/24 10:32 PM
 */
public class FieldReference
        extends Expression {
    private final int fieldIndex;

    public FieldReference(int fieldIndex) {
        super(Optional.empty());
        checkArgument(fieldIndex >= 0, "fieldIndex must be >= 0");

        this.fieldIndex = fieldIndex;
    }

    public int getFieldIndex() {
        return fieldIndex;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitFieldReference(this, context);
    }

    @Override
    public List<Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FieldReference that = (FieldReference) o;

        return fieldIndex == that.fieldIndex;
    }

    @Override
    public int hashCode() {
        return fieldIndex;
    }

    @Override
    public boolean shallowEquals(Node other) {
        if (!Node.sameClass(this, other)) {
            return false;
        }

        return fieldIndex == ((FieldReference) other).fieldIndex;
    }
}
