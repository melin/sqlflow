package io.github.melin.sqlflow.type;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/25 11:40 AM
 */
public class RowType implements Type {

    private final List<Field> fields;

    private RowType(List<Field> fields) {
        this.fields = fields;
    }

    @Override
    public List<Type> getTypeParameters() {
        return Collections.emptyList();
    }

    @Override
    public String getDisplayName() {
        // Convert to standard sql name
        StringBuilder result = new StringBuilder();
        result.append("ROW").append('(');
        for (Field field : fields) {
            String typeDisplayName = field.getType().getDisplayName();
            if (field.getName().isPresent()) {
                // TODO: names are already canonicalized, so they should be printed as delimited identifiers
                result.append(field.getName().get()).append(' ').append(typeDisplayName);
            } else {
                result.append(typeDisplayName);
            }
            result.append(", ");
        }
        result.setLength(result.length() - 2);
        result.append(')');
        return result.toString();
    }

    public static RowType anonymous(List<Type> types) {
        List<Field> fields = types.stream()
                .map(type -> new Field(Optional.empty(), type))
                .collect(Collectors.toList());

        return new RowType(fields);
    }

    @Override
    public boolean isComparable() {
        return true;
    }

    @Override
    public boolean isOrderable() {
        return true;
    }

    public List<Field> getFields() {
        return fields;
    }

    public static RowType from(List<Field> fields)
    {
        return new RowType(fields);
    }

    public static Field field(String name, Type type)
    {
        return new Field(Optional.of(name), type);
    }

    public static Field field(Type type)
    {
        return new Field(Optional.empty(), type);
    }

    public static class Field {
        private final Type type;
        private final Optional<String> name;

        public Field(Optional<String> name, Type type) {
            this.type = requireNonNull(type, "type is null");
            this.name = requireNonNull(name, "name is null");
        }

        public Type getType() {
            return type;
        }

        public Optional<String> getName() {
            return name;
        }
    }
}
