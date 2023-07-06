package io.github.melin.sqlflow.tree.expression;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.tree.Node;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * huaixin 2021/12/21 10:56 AM
 */
public class LambdaArgumentDeclaration
        extends Expression {
    private final Identifier name;

    public LambdaArgumentDeclaration(Identifier name) {
        super(Optional.empty());
        this.name = name;
    }

    public Identifier getName() {
        return name;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitLambdaArgumentDeclaration(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
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
        LambdaArgumentDeclaration that = (LambdaArgumentDeclaration) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public boolean shallowEquals(Node other) {
        if (!Node.sameClass(this, other)) {
            return false;
        }

        return name.equals(((LambdaArgumentDeclaration) other).getName());
    }
}
