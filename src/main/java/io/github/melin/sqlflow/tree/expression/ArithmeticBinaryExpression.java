package io.github.melin.sqlflow.tree.expression;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.tree.Node;
import io.github.melin.sqlflow.tree.NodeLocation;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * huaixin 2021/12/21 11:21 AM
 */
public class ArithmeticBinaryExpression extends Expression {
    public enum Operator {
        ADD("+"),
        SUBTRACT("-"),
        MULTIPLY("*"),
        DIVIDE("/"),
        MODULUS("%");
        private final String value;

        Operator(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    private final Operator operator;
    private final Expression left;
    private final Expression right;

    public ArithmeticBinaryExpression(Operator operator, Expression left, Expression right) {
        this(Optional.empty(), operator, left, right);
    }

    public ArithmeticBinaryExpression(NodeLocation location, Operator operator, Expression left, Expression right) {
        this(Optional.of(location), operator, left, right);
    }

    private ArithmeticBinaryExpression(Optional<NodeLocation> location, Operator operator, Expression left, Expression right) {
        super(location);
        this.operator = operator;
        this.left = left;
        this.right = right;
    }

    public Operator getOperator() {
        return operator;
    }

    public Expression getLeft() {
        return left;
    }

    public Expression getRight() {
        return right;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitArithmeticBinary(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(left, right);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ArithmeticBinaryExpression that = (ArithmeticBinaryExpression) o;
        return (operator == that.operator) &&
                Objects.equals(left, that.left) &&
                Objects.equals(right, that.right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operator, left, right);
    }

    @Override
    public boolean shallowEquals(Node other) {
        if (!Node.sameClass(this, other)) {
            return false;
        }

        return operator == ((ArithmeticBinaryExpression) other).operator;
    }
}
