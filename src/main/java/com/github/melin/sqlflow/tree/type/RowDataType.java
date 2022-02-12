package com.github.melin.sqlflow.tree.type;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.expression.Identifier;
import com.github.melin.sqlflow.tree.Node;
import com.github.melin.sqlflow.tree.NodeLocation;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 11:28 AM
 */
public class RowDataType
        extends DataType {
    private final List<Field> fields;

    public RowDataType(NodeLocation location, List<Field> fields) {
        super(Optional.of(location));
        this.fields = ImmutableList.copyOf(fields);
    }

    public RowDataType(Optional<NodeLocation> location, List<Field> fields) {
        super(location);
        this.fields = ImmutableList.copyOf(fields);
    }

    public List<Field> getFields() {
        return fields;
    }

    @Override
    public List<? extends Node> getChildren() {
        return fields;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitRowDataType(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowDataType that = (RowDataType) o;
        return fields.equals(that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields);
    }

    public static class Field
            extends Node {
        private final Optional<Identifier> name;
        private final DataType type;

        public Field(NodeLocation location, Optional<Identifier> name, DataType type) {
            super(Optional.of(location));

            this.name = requireNonNull(name, "name is null");
            this.type = requireNonNull(type, "type is null");
        }

        public Field(Optional<NodeLocation> location, Optional<Identifier> name, DataType type) {
            super(location);

            this.name = requireNonNull(name, "name is null");
            this.type = requireNonNull(type, "type is null");
        }

        public Optional<Identifier> getName() {
            return name;
        }

        public DataType getType() {
            return type;
        }

        @Override
        public List<? extends Node> getChildren() {
            ImmutableList.Builder<Node> children = ImmutableList.builder();
            name.ifPresent(children::add);
            children.add(type);

            return children.build();
        }

        @Override
        public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
            return visitor.visitRowField(this, context);
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            if (name.isPresent()) {
                builder.append(name.get());
                builder.append(" ");
            }
            builder.append(type);

            return builder.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Field field = (Field) o;
            return name.equals(field.name) &&
                    type.equals(field.type);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, type);
        }

        @Override
        public boolean shallowEquals(Node other) {
            return sameClass(this, other);
        }
    }

    @Override
    public boolean shallowEquals(Node other) {
        return sameClass(this, other);
    }
}
