package io.github.melin.sqlflow.tree.expression;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.tree.Node;
import io.github.melin.sqlflow.tree.NodeLocation;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 11:14 AM
 */
public class LogicalExpression extends Expression {
    public enum Operator {
        AND, OR;

        public Operator flip() {
            switch (this) {
                case AND:
                    return OR;
                case OR:
                    return AND;
            }
            throw new IllegalArgumentException("Unsupported logical expression type: " + this);
        }
    }

    private final Operator operator;
    private final List<Expression> terms;

    public LogicalExpression(Operator operator, List<Expression> terms) {
        this(Optional.empty(), operator, terms);
    }

    public LogicalExpression(NodeLocation location, Operator operator, List<Expression> terms) {
        this(Optional.of(location), operator, terms);
    }

    private LogicalExpression(Optional<NodeLocation> location, Operator operator, List<Expression> terms) {
        super(location);
        requireNonNull(operator, "operator is null");
        checkArgument(terms.size() >= 2, "Expected at least 2 terms");

        this.operator = operator;
        this.terms = ImmutableList.copyOf(terms);
    }

    public Operator getOperator() {
        return operator;
    }

    public List<Expression> getTerms() {
        return terms;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalExpression(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.copyOf(terms);
    }

    public static LogicalExpression and(Expression left, Expression right) {
        return new LogicalExpression(Optional.empty(), Operator.AND, ImmutableList.of(left, right));
    }

    public static LogicalExpression or(Expression left, Expression right) {
        return new LogicalExpression(Optional.empty(), Operator.OR, ImmutableList.of(left, right));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalExpression that = (LogicalExpression) o;
        return operator == that.operator && Objects.equals(terms, that.terms);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operator, terms);
    }

    @Override
    public boolean shallowEquals(Node other) {
        if (!Node.sameClass(this, other)) {
            return false;
        }

        return operator == ((LogicalExpression) other).operator;
    }
}
