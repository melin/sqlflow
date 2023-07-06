package io.github.melin.sqlflow.tree.join;

import io.github.melin.sqlflow.tree.expression.Expression;
import io.github.melin.sqlflow.tree.Node;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/19 11:02 AM
 */
public class JoinOn extends JoinCriteria {
    private final Expression expression;

    public JoinOn(Expression expression) {
        this.expression = requireNonNull(expression, "expression is null");
    }

    public Expression getExpression() {
        return expression;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        JoinOn o = (JoinOn) obj;
        return Objects.equals(expression, o.expression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expression);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .addValue(expression)
                .toString();
    }

    @Override
    public List<Node> getNodes() {
        return ImmutableList.of(expression);
    }
}

