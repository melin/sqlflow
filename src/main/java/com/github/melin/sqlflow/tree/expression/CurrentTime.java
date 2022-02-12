package com.github.melin.sqlflow.tree.expression;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.Node;
import com.github.melin.sqlflow.tree.NodeLocation;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 10:37 AM
 */
public class CurrentTime extends Expression {
    private final Function function;
    private final Integer precision;

    public enum Function {
        TIME("current_time"),
        DATE("current_date"),
        TIMESTAMP("current_timestamp"),
        LOCALTIME("localtime"),
        LOCALTIMESTAMP("localtimestamp");

        private final String name;

        Function(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    public CurrentTime(Function function) {
        this(Optional.empty(), function, null);
    }

    public CurrentTime(NodeLocation location, Function function) {
        this(Optional.of(location), function, null);
    }

    public CurrentTime(Function function, Integer precision) {
        this(Optional.empty(), function, precision);
    }

    public CurrentTime(NodeLocation location, Function function, Integer precision) {
        this(Optional.of(location), function, precision);
    }

    private CurrentTime(Optional<NodeLocation> location, Function function, Integer precision) {
        super(location);
        requireNonNull(function, "function is null");
        this.function = function;
        this.precision = precision;
    }

    public Function getFunction() {
        return function;
    }

    public Integer getPrecision() {
        return precision;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCurrentTime(this, context);
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
        if ((o == null) || (getClass() != o.getClass())) {
            return false;
        }
        CurrentTime that = (CurrentTime) o;
        return (function == that.function) &&
                Objects.equals(precision, that.precision);
    }

    @Override
    public int hashCode() {
        return Objects.hash(function, precision);
    }

    @Override
    public boolean shallowEquals(Node other) {
        if (!Node.sameClass(this, other)) {
            return false;
        }

        CurrentTime otherNode = (CurrentTime) other;
        return (function == otherNode.function) &&
                Objects.equals(precision, otherNode.precision);
    }
}
