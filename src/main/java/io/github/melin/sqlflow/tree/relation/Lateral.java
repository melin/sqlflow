package io.github.melin.sqlflow.tree.relation;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.tree.Node;
import io.github.melin.sqlflow.tree.NodeLocation;
import io.github.melin.sqlflow.tree.statement.Query;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 1:12 PM
 */
public final class Lateral extends Relation {
    private final Query query;

    public Lateral(Query query) {
        this(Optional.empty(), query);
    }

    public Lateral(NodeLocation location, Query query) {
        this(Optional.of(location), query);
    }

    private Lateral(Optional<NodeLocation> location, Query query) {
        super(location);
        this.query = requireNonNull(query, "query is null");
    }

    public Query getQuery() {
        return query;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitLateral(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(query);
    }

    @Override
    public String toString() {
        return "LATERAL(" + query + ")";
    }

    @Override
    public int hashCode() {
        return Objects.hash(query);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Lateral other = (Lateral) obj;
        return Objects.equals(this.query, other.query);
    }

    @Override
    public boolean shallowEquals(Node other) {
        return Node.sameClass(this, other);
    }
}

