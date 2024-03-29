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
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 12:54 PM
 */
public class MergeInsert extends MergeCase {
    private final List<Identifier> columns;
    private final List<Expression> values;

    public MergeInsert(Optional<Expression> expression, List<Identifier> columns, List<Expression> values) {
        this(Optional.empty(), expression, columns, values);
    }

    public MergeInsert(NodeLocation location, Optional<Expression> expression, List<Identifier> columns, List<Expression> values) {
        this(Optional.of(location), expression, columns, values);
    }

    public MergeInsert(Optional<NodeLocation> location, Optional<Expression> expression, List<Identifier> columns, List<Expression> values) {
        super(location, expression);
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.values = ImmutableList.copyOf(requireNonNull(values, "values is null"));
    }

    public List<Identifier> getColumns() {
        return columns;
    }

    public List<Expression> getValues() {
        return values;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitMergeInsert(this, context);
    }

    @Override
    public List<Identifier> getSetColumns() {
        return columns;
    }

    @Override
    public List<Expression> getSetExpressions() {
        return values;
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> builder = ImmutableList.builder();
        expression.ifPresent(builder::add);
        builder.addAll(columns);
        builder.addAll(values);
        return builder.build();
    }

    @Override
    public int hashCode() {
        return Objects.hash(expression, columns, values);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MergeInsert o = (MergeInsert) obj;
        return Objects.equals(expression, o.expression) &&
                Objects.equals(columns, o.columns) &&
                Objects.equals(values, o.values);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("expression", expression.orElse(null))
                .add("columns", columns)
                .add("values", values)
                .omitNullValues()
                .toString();
    }
}
