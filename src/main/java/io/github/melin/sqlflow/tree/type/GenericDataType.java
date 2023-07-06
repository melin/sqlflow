package io.github.melin.sqlflow.tree.type;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.tree.expression.Identifier;
import io.github.melin.sqlflow.tree.Node;
import io.github.melin.sqlflow.tree.NodeLocation;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 11:30 AM
 */
public class GenericDataType extends DataType {
    private final Identifier name;
    private final List<DataTypeParameter> arguments;

    public GenericDataType(NodeLocation location, Identifier name, List<DataTypeParameter> arguments) {
        super(Optional.of(location));
        this.name = requireNonNull(name, "name is null");
        this.arguments = requireNonNull(arguments, "arguments is null");
    }

    public GenericDataType(Optional<NodeLocation> location, Identifier name, List<DataTypeParameter> arguments) {
        super(location);
        this.name = requireNonNull(name, "name is null");
        this.arguments = requireNonNull(arguments, "arguments is null");
    }

    public Identifier getName() {
        return name;
    }

    public List<DataTypeParameter> getArguments() {
        return arguments;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.<Node>builder()
                .add(name)
                .addAll(arguments)
                .build();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitGenericDataType(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GenericDataType that = (GenericDataType) o;
        return name.equals(that.name) &&
                arguments.equals(that.arguments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, arguments);
    }

    @Override
    public boolean shallowEquals(Node other) {
        return sameClass(this, other);
    }
}
