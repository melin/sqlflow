package io.github.melin.sqlflow.tree.relation;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.tree.Node;
import io.github.melin.sqlflow.tree.NodeLocation;
import io.github.melin.sqlflow.tree.expression.Expression;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/27 7:58 PM
 */
public final class Unnest extends Relation {
    private final List<Expression> expressions;
    private final boolean withOrdinality;

    public Unnest(List<Expression> expressions, boolean withOrdinality) {
        this(Optional.empty(), expressions, withOrdinality);
    }

    public Unnest(NodeLocation location, List<Expression> expressions, boolean withOrdinality) {
        this(Optional.of(location), expressions, withOrdinality);
    }

    private Unnest(Optional<NodeLocation> location, List<Expression> expressions, boolean withOrdinality) {
        super(location);
        requireNonNull(expressions, "expressions is null");
        this.expressions = ImmutableList.copyOf(expressions);
        this.withOrdinality = withOrdinality;
    }

    public List<Expression> getExpressions() {
        return expressions;
    }

    public boolean isWithOrdinality() {
        return withOrdinality;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitUnnest(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return expressions;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("expressions", expressions)
                .add("withOrdinality", withOrdinality)
                .toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(expressions, withOrdinality);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Unnest other = (Unnest) obj;
        return Objects.equals(expressions, other.expressions) && withOrdinality == other.withOrdinality;
    }

    @Override
    public boolean shallowEquals(Node other) {
        if (!Node.sameClass(this, other)) {
            return false;
        }

        Unnest otherNode = (Unnest) other;
        return withOrdinality == otherNode.withOrdinality;
    }
}
