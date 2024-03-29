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
 * huaixin 2021/12/21 1:09 PM
 */
public class Except extends SetOperation {
    private final Relation left;
    private final Relation right;

    public Except(Relation left, Relation right, boolean distinct) {
        this(Optional.empty(), left, right, distinct);
    }

    public Except(NodeLocation location, Relation left, Relation right, boolean distinct) {
        this(Optional.of(location), left, right, distinct);
    }

    private Except(Optional<NodeLocation> location, Relation left, Relation right, boolean distinct) {
        super(location, distinct);
        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");

        this.left = left;
        this.right = right;
    }

    public Relation getLeft() {
        return left;
    }

    public Relation getRight() {
        return right;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitExcept(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(left, right);
    }

    @Override
    public List<Relation> getRelations() {
        return ImmutableList.of(left, right);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("left", left)
                .add("right", right)
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
        Except o = (Except) obj;
        return Objects.equals(left, o.left) &&
                Objects.equals(right, o.right) &&
                Objects.equals(isDistinct(), o.isDistinct());
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right, isDistinct());
    }

    @Override
    public boolean shallowEquals(Node other) {
        if (!Node.sameClass(this, other)) {
            return false;
        }

        return this.isDistinct() == ((Except) other).isDistinct();
    }
}
