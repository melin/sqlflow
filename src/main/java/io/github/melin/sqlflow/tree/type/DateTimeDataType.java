package io.github.melin.sqlflow.tree.type;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.tree.Node;
import io.github.melin.sqlflow.tree.NodeLocation;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 11:33 AM
 */
public class DateTimeDataType
        extends DataType {
    public enum Type {
        TIMESTAMP, TIME
    }

    private final Type type;
    private final boolean withTimeZone;
    private final Optional<DataTypeParameter> precision;

    public DateTimeDataType(NodeLocation location, Type type, boolean withTimeZone, Optional<DataTypeParameter> precision) {
        this(Optional.of(location), type, withTimeZone, precision);
    }

    public DateTimeDataType(Optional<NodeLocation> location, Type type, boolean withTimeZone, Optional<DataTypeParameter> precision) {
        super(location);
        this.type = requireNonNull(type, "type is null");
        this.withTimeZone = withTimeZone;
        this.precision = requireNonNull(precision, "precision is null");
    }

    public Type getType() {
        return type;
    }

    public boolean isWithTimeZone() {
        return withTimeZone;
    }

    public Optional<DataTypeParameter> getPrecision() {
        return precision;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDateTimeType(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DateTimeDataType that = (DateTimeDataType) o;
        return withTimeZone == that.withTimeZone &&
                type == that.type &&
                precision.equals(that.precision);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, withTimeZone, precision);
    }

    @Override
    public boolean shallowEquals(Node other) {
        if (!sameClass(this, other)) {
            return false;
        }

        DateTimeDataType otherType = (DateTimeDataType) other;
        return type.equals(otherType.type) &&
                withTimeZone == otherType.withTimeZone &&
                precision.equals(otherType.precision);
    }
}
