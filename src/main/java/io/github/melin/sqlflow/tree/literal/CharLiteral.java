package io.github.melin.sqlflow.tree.literal;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.tree.Node;
import io.github.melin.sqlflow.tree.NodeLocation;
import com.google.common.base.CharMatcher;
import io.airlift.slice.Slice;

import java.util.Objects;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/19 9:27 PM
 */
public class CharLiteral extends Literal {
    private final String value;
    private final Slice slice;

    public CharLiteral(String value) {
        this(Optional.empty(), value);
    }

    public CharLiteral(NodeLocation location, String value) {
        this(Optional.of(location), value);
    }

    public CharLiteral(Optional<NodeLocation> location, String value) {
        super(location);
        requireNonNull(value, "value is null");
        this.value = value;
        this.slice = utf8Slice(CharMatcher.is(' ').trimTrailingFrom(value));
    }

    public String getValue() {
        return value;
    }

    public Slice getSlice() {
        return slice;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCharLiteral(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CharLiteral that = (CharLiteral) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public boolean shallowEquals(Node other) {
        if (!Node.sameClass(this, other)) {
            return false;
        }

        return Objects.equals(value, ((CharLiteral) other).value);
    }
}
