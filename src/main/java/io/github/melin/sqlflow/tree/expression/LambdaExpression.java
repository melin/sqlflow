package io.github.melin.sqlflow.tree.expression;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.tree.Node;
import io.github.melin.sqlflow.tree.NodeLocation;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 11:05 AM
 */
public class LambdaExpression
        extends Expression {
    private final List<LambdaArgumentDeclaration> arguments;
    private final Expression body;

    public LambdaExpression(List<LambdaArgumentDeclaration> arguments, Expression body) {
        this(Optional.empty(), arguments, body);
    }

    public LambdaExpression(NodeLocation location, List<LambdaArgumentDeclaration> arguments, Expression body) {
        this(Optional.of(location), arguments, body);
    }

    private LambdaExpression(Optional<NodeLocation> location, List<LambdaArgumentDeclaration> arguments, Expression body) {
        super(location);
        this.arguments = requireNonNull(arguments, "arguments is null");
        this.body = requireNonNull(body, "body is null");
    }

    public List<LambdaArgumentDeclaration> getArguments() {
        return arguments;
    }

    public Expression getBody() {
        return body;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitLambdaExpression(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.addAll(arguments);
        nodes.add(body);
        return nodes.build();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        LambdaExpression that = (LambdaExpression) obj;
        return Objects.equals(arguments, that.arguments) &&
                Objects.equals(body, that.body);
    }

    @Override
    public int hashCode() {
        return Objects.hash(arguments, body);
    }

    @Override
    public boolean shallowEquals(Node other) {
        return Node.sameClass(this, other);
    }
}
