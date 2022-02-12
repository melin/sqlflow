package com.github.melin.sqlflow.tree.type;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.Node;
import com.github.melin.sqlflow.tree.NodeLocation;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 11:33 AM
 */
public class IntervalDayTimeDataType extends DataType {
    public enum Field {
        YEAR,
        MONTH,
        DAY,
        HOUR,
        MINUTE,
        SECOND,
    }

    private final Field from;
    private final Field to;

    public IntervalDayTimeDataType(NodeLocation location, Field from, Field to) {
        this(Optional.of(location), from, to);
    }

    public IntervalDayTimeDataType(Optional<NodeLocation> location, Field from, Field to) {
        super(location);
        this.from = requireNonNull(from, "from is null");
        this.to = requireNonNull(to, "to is null");
    }

    public Field getFrom() {
        return from;
    }

    public Field getTo() {
        return to;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitIntervalDataType(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IntervalDayTimeDataType that = (IntervalDayTimeDataType) o;
        return from == that.from &&
                to == that.to;
    }

    @Override
    public int hashCode() {
        return Objects.hash(from, to);
    }

    @Override
    public boolean shallowEquals(Node other) {
        if (!sameClass(this, other)) {
            return false;
        }

        IntervalDayTimeDataType otherType = (IntervalDayTimeDataType) other;
        return from.equals(otherType.from) &&
                to == otherType.to;
    }
}
