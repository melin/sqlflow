package io.github.melin.sqlflow.tree.literal;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.tree.Node;
import io.github.melin.sqlflow.tree.NodeLocation;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/19 9:26 PM
 */
public class IntervalLiteral extends Literal {
    public enum Sign {
        POSITIVE {
            @Override
            public int multiplier() {
                return 1;
            }
        },
        NEGATIVE {
            @Override
            public int multiplier() {
                return -1;
            }
        };

        public abstract int multiplier();
    }

    public enum IntervalField {
        YEAR, MONTH, DAY, HOUR, MINUTE, SECOND
    }

    private final String value;
    private final Sign sign;
    private final IntervalField startField;
    private final Optional<IntervalField> endField;

    public IntervalLiteral(String value, Sign sign, IntervalField startField) {
        this(Optional.empty(), value, sign, startField, Optional.empty());
    }

    public IntervalLiteral(String value, Sign sign, IntervalField startField, Optional<IntervalField> endField) {
        this(Optional.empty(), value, sign, startField, endField);
    }

    public IntervalLiteral(NodeLocation location, String value, Sign sign, IntervalField startField, Optional<IntervalField> endField) {
        this(Optional.of(location), value, sign, startField, endField);
    }

    private IntervalLiteral(Optional<NodeLocation> location, String value, Sign sign, IntervalField startField, Optional<IntervalField> endField) {
        super(location);
        requireNonNull(value, "value is null");
        requireNonNull(sign, "sign is null");
        requireNonNull(startField, "startField is null");
        requireNonNull(endField, "endField is null");

        this.value = value;
        this.sign = sign;
        this.startField = startField;
        this.endField = endField;
    }

    public String getValue() {
        return value;
    }

    public Sign getSign() {
        return sign;
    }

    public IntervalField getStartField() {
        return startField;
    }

    public Optional<IntervalField> getEndField() {
        return endField;
    }

    public boolean isYearToMonth() {
        return startField == IntervalField.YEAR || startField == IntervalField.MONTH;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitIntervalLiteral(this, context);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, sign, startField, endField);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        IntervalLiteral other = (IntervalLiteral) obj;
        return Objects.equals(this.value, other.value) &&
                this.sign == other.sign &&
                this.startField == other.startField &&
                Objects.equals(this.endField, other.endField);
    }

    @Override
    public boolean shallowEquals(Node other) {
        if (!Node.sameClass(this, other)) {
            return false;
        }

        IntervalLiteral otherLiteral = (IntervalLiteral) other;
        return Objects.equals(this.value, otherLiteral.value) &&
                this.sign == otherLiteral.sign &&
                this.startField == otherLiteral.startField &&
                Objects.equals(this.endField, otherLiteral.endField);
    }
}
