package io.github.melin.sqlflow.tree.expression;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.tree.Node;
import io.github.melin.sqlflow.tree.NodeLocation;
import io.github.melin.sqlflow.tree.literal.IntervalLiteral;
import io.github.melin.sqlflow.tree.literal.StringLiteral;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 10:34 AM
 */
public class AtTimeZone
        extends Expression {
    private final Expression value;
    private final Expression timeZone;

    public AtTimeZone(Expression value, Expression timeZone) {
        this(Optional.empty(), value, timeZone);
    }

    public AtTimeZone(NodeLocation location, Expression value, Expression timeZone) {
        this(Optional.of(location), value, timeZone);
    }

    private AtTimeZone(Optional<NodeLocation> location, Expression value, Expression timeZone) {
        super(location);
        checkArgument(timeZone instanceof IntervalLiteral || timeZone instanceof StringLiteral, "timeZone must be IntervalLiteral or StringLiteral");
        this.value = requireNonNull(value, "value is null");
        this.timeZone = requireNonNull(timeZone, "timeZone is null");
    }

    public Expression getValue() {
        return value;
    }

    public Expression getTimeZone() {
        return timeZone;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAtTimeZone(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(value, timeZone);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, timeZone);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        AtTimeZone atTimeZone = (AtTimeZone) obj;
        return Objects.equals(value, atTimeZone.value) && Objects.equals(timeZone, atTimeZone.timeZone);
    }

    @Override
    public boolean shallowEquals(Node other) {
        return Node.sameClass(this, other);
    }
}
