package io.github.melin.sqlflow.tree.merge;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.tree.expression.Identifier;
import io.github.melin.sqlflow.tree.Node;
import io.github.melin.sqlflow.tree.NodeLocation;
import io.github.melin.sqlflow.tree.expression.Expression;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 12:55 PM
 */
public class MergeUpdate extends MergeCase {
    private final List<Assignment> assignments;

    public MergeUpdate(Optional<Expression> expression, List<Assignment> assignments) {
        this(Optional.empty(), expression, assignments);
    }

    public MergeUpdate(NodeLocation location, Optional<Expression> expression, List<Assignment> assignments) {
        this(Optional.of(location), expression, assignments);
    }

    public MergeUpdate(Optional<NodeLocation> location, Optional<Expression> expression, List<Assignment> assignments) {
        super(location, expression);
        this.assignments = ImmutableList.copyOf(requireNonNull(assignments, "assignments is null"));
    }

    public List<Assignment> getAssignments() {
        return assignments;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitMergeUpdate(this, context);
    }

    @Override
    public List<Identifier> getSetColumns() {
        return assignments.stream()
                .map(Assignment::getTarget)
                .collect(toImmutableList());
    }

    @Override
    public List<Expression> getSetExpressions() {
        return assignments.stream()
                .map(Assignment::getValue)
                .collect(toImmutableList());
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> builder = ImmutableList.builder();
        expression.ifPresent(builder::add);
        assignments.forEach(assignment -> {
            builder.add(assignment.getTarget());
            builder.add(assignment.getValue());
        });
        return builder.build();
    }

    @Override
    public int hashCode() {
        return Objects.hash(expression, assignments);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MergeUpdate o = (MergeUpdate) obj;
        return Objects.equals(expression, o.expression) &&
                Objects.equals(assignments, o.assignments);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("expression", expression.orElse(null))
                .add("assignments", assignments)
                .omitNullValues()
                .toString();
    }

    public static class Assignment {
        private final Identifier target;
        private final Expression value;

        public Assignment(Identifier target, Expression value) {
            this.target = requireNonNull(target, "target is null");
            this.value = requireNonNull(value, "value is null");
        }

        public Identifier getTarget() {
            return target;
        }

        public Expression getValue() {
            return value;
        }

        @Override
        public int hashCode() {
            return Objects.hash(target, value);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Assignment o = (Assignment) obj;
            return Objects.equals(target, o.target) &&
                    Objects.equals(value, o.value);
        }

        @Override
        public String toString() {
            return toStringHelper(this)
                    .add("target", target)
                    .add("value", value)
                    .toString();
        }
    }
}
