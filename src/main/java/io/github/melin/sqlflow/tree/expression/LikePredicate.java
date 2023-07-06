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
 * huaixin 2021/12/21 11:24 AM
 */
public class LikePredicate extends Expression {
    private final Expression value;
    private final Expression pattern;
    private final Optional<Expression> escape;

    public LikePredicate(Expression value, Expression pattern, Expression escape) {
        this(Optional.empty(), value, pattern, Optional.of(escape));
    }

    public LikePredicate(NodeLocation location, Expression value, Expression pattern, Optional<Expression> escape) {
        this(Optional.of(location), value, pattern, escape);
    }

    public LikePredicate(Expression value, Expression pattern, Optional<Expression> escape) {
        this(Optional.empty(), value, pattern, escape);
    }

    private LikePredicate(Optional<NodeLocation> location, Expression value, Expression pattern, Optional<Expression> escape) {
        super(location);
        requireNonNull(value, "value is null");
        requireNonNull(pattern, "pattern is null");
        requireNonNull(escape, "escape is null");

        this.value = value;
        this.pattern = pattern;
        this.escape = escape;
    }

    public Expression getValue() {
        return value;
    }

    public Expression getPattern() {
        return pattern;
    }

    public Optional<Expression> getEscape() {
        return escape;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitLikePredicate(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> result = ImmutableList.<Node>builder()
                .add(value)
                .add(pattern);

        escape.ifPresent(result::add);

        return result.build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LikePredicate that = (LikePredicate) o;
        return Objects.equals(value, that.value) &&
                Objects.equals(pattern, that.pattern) &&
                Objects.equals(escape, that.escape);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, pattern, escape);
    }

    @Override
    public boolean shallowEquals(Node other) {
        return Node.sameClass(this, other);
    }
}
