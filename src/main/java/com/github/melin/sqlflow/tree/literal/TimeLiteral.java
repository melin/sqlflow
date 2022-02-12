package com.github.melin.sqlflow.tree.literal;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.Node;
import com.github.melin.sqlflow.tree.NodeLocation;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/19 9:25 PM
 */
public class TimeLiteral extends Literal {
    private final String value;

    public TimeLiteral(String value) {
        this(Optional.empty(), value);
    }

    public TimeLiteral(NodeLocation location, String value) {
        this(Optional.of(location), value);
    }

    private TimeLiteral(Optional<NodeLocation> location, String value) {
        super(location);
        requireNonNull(value, "value is null");
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitTimeLiteral(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TimeLiteral that = (TimeLiteral) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean shallowEquals(Node other) {
        if (!Node.sameClass(this, other)) {
            return false;
        }

        TimeLiteral otherLiteral = (TimeLiteral) other;
        return Objects.equals(this.value, otherLiteral.value);
    }
}
