package com.github.melin.sqlflow.tree.literal;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.Node;
import com.github.melin.sqlflow.tree.NodeLocation;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/19 9:31 PM
 */
public class BooleanLiteral
        extends Literal {
    public static final BooleanLiteral TRUE_LITERAL = new BooleanLiteral(Optional.empty(), "true");
    public static final BooleanLiteral FALSE_LITERAL = new BooleanLiteral(Optional.empty(), "false");

    private final boolean value;

    public BooleanLiteral(String value) {
        this(Optional.empty(), value);
    }

    public BooleanLiteral(NodeLocation location, String value) {
        this(Optional.of(location), value);
    }

    private BooleanLiteral(Optional<NodeLocation> location, String value) {
        super(location);
        requireNonNull(value, "value is null");
        checkArgument(value.toLowerCase(ENGLISH).equals("true") || value.toLowerCase(ENGLISH).equals("false"));

        this.value = value.toLowerCase(ENGLISH).equals("true");
    }

    public boolean getValue() {
        return value;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitBooleanLiteral(this, context);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        BooleanLiteral other = (BooleanLiteral) obj;
        return Objects.equals(this.value, other.value);
    }

    @Override
    public boolean shallowEquals(Node other) {
        if (!Node.sameClass(this, other)) {
            return false;
        }

        return value == ((BooleanLiteral) other).value;
    }
}
