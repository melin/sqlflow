package io.github.melin.sqlflow.tree.relation;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.tree.Node;
import io.github.melin.sqlflow.tree.NodeLocation;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 1:08 PM
 */
public class Union extends SetOperation {
    private final List<Relation> relations;

    public Union(List<Relation> relations, boolean distinct) {
        this(Optional.empty(), relations, distinct);
    }

    public Union(NodeLocation location, List<Relation> relations, boolean distinct) {
        this(Optional.of(location), relations, distinct);
    }

    private Union(Optional<NodeLocation> location, List<Relation> relations, boolean distinct) {
        super(location, distinct);
        requireNonNull(relations, "relations is null");

        this.relations = ImmutableList.copyOf(relations);
    }

    @Override
    public List<Relation> getRelations() {
        return relations;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitUnion(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return relations;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("relations", relations)
                .add("distinct", isDistinct())
                .toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        Union o = (Union) obj;
        return Objects.equals(relations, o.relations) &&
                Objects.equals(isDistinct(), o.isDistinct());
    }

    @Override
    public int hashCode() {
        return Objects.hash(relations, isDistinct());
    }

    @Override
    public boolean shallowEquals(Node other) {
        if (!Node.sameClass(this, other)) {
            return false;
        }

        return this.isDistinct() == ((Union) other).isDistinct();
    }
}
