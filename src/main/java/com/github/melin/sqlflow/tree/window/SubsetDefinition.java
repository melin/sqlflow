package com.github.melin.sqlflow.tree.window;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.expression.Identifier;
import com.github.melin.sqlflow.tree.Node;
import com.github.melin.sqlflow.tree.NodeLocation;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/19 9:53 PM
 */
public class SubsetDefinition extends Node {
    private final Identifier name;
    private final List<Identifier> identifiers;

    public SubsetDefinition(Identifier name, List<Identifier> identifiers) {
        this(Optional.empty(), name, identifiers);
    }

    public SubsetDefinition(NodeLocation location, Identifier name, List<Identifier> identifiers) {
        this(Optional.of(location), name, identifiers);
    }

    private SubsetDefinition(Optional<NodeLocation> location, Identifier name, List<Identifier> identifiers) {
        super(location);
        this.name = requireNonNull(name, "name is null");
        requireNonNull(identifiers, "identifiers is null");
        checkArgument(!identifiers.isEmpty(), "identifiers is empty");
        this.identifiers = identifiers;
    }

    public Identifier getName() {
        return name;
    }

    public List<Identifier> getIdentifiers() {
        return identifiers;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSubsetDefinition(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return identifiers;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("name", name)
                .add("identifiers", identifiers)
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

        SubsetDefinition that = (SubsetDefinition) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(identifiers, that.identifiers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, identifiers);
    }

    @Override
    public boolean shallowEquals(Node other) {
        if (!sameClass(this, other)) {
            return false;
        }

        return Objects.equals(name, ((SubsetDefinition) other).name);
    }
}
