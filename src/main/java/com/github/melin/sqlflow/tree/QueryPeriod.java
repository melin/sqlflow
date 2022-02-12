package com.github.melin.sqlflow.tree;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.expression.Expression;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/19 10:19 PM
 */
public class QueryPeriod extends Node {
    private final Optional<Expression> start;
    private final Optional<Expression> end;
    private final RangeType rangeType;

    public enum RangeType {
        TIMESTAMP,
        VERSION
    }

    public QueryPeriod(NodeLocation location, RangeType rangeType, Expression end) {
        this(location, rangeType, Optional.empty(), Optional.of(end));
    }

    private QueryPeriod(NodeLocation location, RangeType rangeType, Optional<Expression> start, Optional<Expression> end) {
        super(Optional.of(location));
        this.rangeType = requireNonNull(rangeType, "rangeType is null");
        this.start = start;
        this.end = end;
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        if (start.isPresent()) {
            nodes.add(start.get());
        }
        if (end.isPresent()) {
            nodes.add(end.get());
        }
        return nodes.build();
    }

    public Optional<Expression> getStart() {
        return start;
    }

    public Optional<Expression> getEnd() {
        return end;
    }

    public RangeType getRangeType() {
        return rangeType;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitQueryPeriod(this, context);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        QueryPeriod o = (QueryPeriod) obj;
        return Objects.equals(rangeType, o.rangeType) &&
                Objects.equals(start, o.start) &&
                Objects.equals(end, o.end);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rangeType, start, end);
    }

    @Override
    public String toString() {
        return "FOR " + rangeType.toString() + " AS OF " + end.get().toString();
    }
}
