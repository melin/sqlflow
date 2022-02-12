package com.github.melin.sqlflow.tree.statement;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.Node;
import com.github.melin.sqlflow.tree.NodeLocation;
import com.github.melin.sqlflow.tree.relation.Table;
import com.github.melin.sqlflow.tree.UpdateAssignment;
import com.github.melin.sqlflow.tree.expression.Expression;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 12:51 PM
 */
public class Update extends Statement {
    private final Table table;
    private final List<UpdateAssignment> assignments;
    private final Optional<Expression> where;

    public Update(Table table, List<UpdateAssignment> assignments, Optional<Expression> where) {
        this(Optional.empty(), table, assignments, where);
    }

    public Update(NodeLocation location, Table table, List<UpdateAssignment> assignments, Optional<Expression> where) {
        this(Optional.of(location), table, assignments, where);
    }

    private Update(Optional<NodeLocation> location, Table table, List<UpdateAssignment> assignments, Optional<Expression> where) {
        super(location);
        this.table = requireNonNull(table, "table is null");
        this.assignments = requireNonNull(assignments, "assignments is null");
        this.where = requireNonNull(where, "where is null");
    }

    public Table getTable() {
        return table;
    }

    public List<UpdateAssignment> getAssignments() {
        return assignments;
    }

    public Optional<Expression> getWhere() {
        return where;
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.addAll(assignments);
        where.ifPresent(nodes::add);
        return nodes.build();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitUpdate(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Update update = (Update) o;
        return table.equals(update.table) &&
                assignments.equals(update.assignments) &&
                where.equals(update.where);
    }

    @Override
    public int hashCode() {
        return Objects.hash(table, assignments, where);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("table", table)
                .add("assignments", assignments)
                .add("where", where.orElse(null))
                .omitNullValues()
                .toString();
    }
}
