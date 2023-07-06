package io.github.melin.sqlflow.tree.relation;

import io.github.melin.sqlflow.AstVisitor;
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
 * huaixin 2021/12/21 1:17 PM
 */
public class SampledRelation
        extends Relation {
    public enum Type {
        BERNOULLI,
        SYSTEM
    }

    private final Relation relation;
    private final Type type;
    private final Expression samplePercentage;

    public SampledRelation(Relation relation, Type type, Expression samplePercentage) {
        this(Optional.empty(), relation, type, samplePercentage);
    }

    public SampledRelation(NodeLocation location, Relation relation, Type type, Expression samplePercentage) {
        this(Optional.of(location), relation, type, samplePercentage);
    }

    private SampledRelation(Optional<NodeLocation> location, Relation relation, Type type, Expression samplePercentage) {
        super(location);
        this.relation = requireNonNull(relation, "relation is null");
        this.type = requireNonNull(type, "type is null");
        this.samplePercentage = requireNonNull(samplePercentage, "samplePercentage is null");
    }

    public Relation getRelation() {
        return relation;
    }

    public Type getType() {
        return type;
    }

    public Expression getSamplePercentage() {
        return samplePercentage;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSampledRelation(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(relation, samplePercentage);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("relation", relation)
                .add("type", type)
                .add("samplePercentage", samplePercentage)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SampledRelation that = (SampledRelation) o;
        return Objects.equals(relation, that.relation) &&
                type == that.type &&
                Objects.equals(samplePercentage, that.samplePercentage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(relation, type, samplePercentage);
    }
}
