package com.github.melin.sqlflow.analyzer;

import com.github.melin.sqlflow.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.concurrent.Immutable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * TODO: this needs to be merged with RowType at some point (when the type system is unified)
 */
@Immutable
public class RelationType {
    private final List<Field> visibleFields;
    private final List<Field> allFields;

    private final Map<Field, Integer> fieldIndexes;

    public RelationType(Field... fields) {
        this(ImmutableList.copyOf(fields));
    }

    public RelationType(List<Field> fields) {
        requireNonNull(fields, "fields is null");
        this.allFields = ImmutableList.copyOf(fields);
        this.visibleFields = fields.stream()
                .collect(toImmutableList());

        int index = 0;
        ImmutableMap.Builder<Field, Integer> builder = ImmutableMap.builder();
        for (Field field : fields) {
            builder.put(field, index++);
        }
        fieldIndexes = builder.build();
    }

    /**
     * Gets the index of the specified field.
     *
     * @throws IllegalArgumentException when field is not found
     */
    public int indexOf(Field field) {
        requireNonNull(field, "field cannot be null");
        Integer index = fieldIndexes.get(field);
        checkArgument(index != null, "Field '%s' not found", field);
        return index;
    }

    /**
     * Gets the field at the specified index.
     */
    public Field getFieldByIndex(int fieldIndex) {
        return allFields.get(fieldIndex);
    }

    /**
     * Gets only the visible fields.
     * No assumptions should be made about the order of the fields returned from this method.
     * To obtain the index of a field, call indexOf.
     */
    public Collection<Field> getVisibleFields() {
        return visibleFields;
    }

    public int getVisibleFieldCount() {
        return visibleFields.size();
    }

    /**
     * Gets all fields including hidden fields.
     * No assumptions should be made about the order of the fields returned from this method.
     * To obtain the index of a field, call indexOf.
     */
    public Collection<Field> getAllFields() {
        return ImmutableSet.copyOf(allFields);
    }

    /**
     * Gets the count of all fields including hidden fields.
     */
    public int getAllFieldCount() {
        return allFields.size();
    }

    /**
     * Gets all visible fields whose relation alias matches given prefix.
     */
    public List<Field> resolveVisibleFieldsWithRelationPrefix(Optional<QualifiedName> prefix) {
        return visibleFields.stream()
                .filter(input -> input.matchesPrefix(prefix))
                .collect(toImmutableList());
    }

    /**
     * Gets the index of all columns matching the specified name
     */
    public List<Field> resolveFields(QualifiedName name) {
        return allFields.stream()
                .filter(input -> input.canResolve(name))
                .collect(toImmutableList());
    }

    public boolean canResolve(QualifiedName name) {
        return !resolveFields(name).isEmpty();
    }

    /**
     * Creates a new tuple descriptor containing all fields from this tuple descriptor
     * and all fields from the specified tuple descriptor.
     */
    public RelationType joinWith(RelationType other) {
        List<Field> fields = ImmutableList.<Field>builder()
                .addAll(this.allFields)
                .addAll(other.allFields)
                .build();

        return new RelationType(fields);
    }

    /**
     * Creates a new tuple descriptor with the relation, and, optionally, the columns aliased.
     */
    public RelationType withAlias(String relationAlias, List<String> columnAliases) {
        if (columnAliases != null) {
            checkArgument(columnAliases.size() == visibleFields.size(),
                    "Column alias list has %s entries but '%s' has %s columns available",
                    columnAliases.size(),
                    relationAlias,
                    visibleFields.size());
        }

        int aliasIndex = 0;
        ImmutableList.Builder<Field> fieldsBuilder = ImmutableList.builder();
        for (Field field : allFields) {
            Optional<String> columnAlias = field.getName();
            if (columnAliases == null) {
                fieldsBuilder.add(Field.newQualified(
                        QualifiedName.of(relationAlias),
                        columnAlias,
                        field.getOriginTable(),
                        field.getOriginColumnName(),
                        field.isAliased()));
            } else {
                // hidden fields are not exposed when there are column aliases
                columnAlias = Optional.of(columnAliases.get(aliasIndex));
                aliasIndex++;
                fieldsBuilder.add(Field.newQualified(
                        QualifiedName.of(relationAlias),
                        columnAlias,
                        field.getOriginTable(),
                        field.getOriginColumnName(),
                        field.isAliased()));
            }
        }

        return new RelationType(fieldsBuilder.build());
    }

    /**
     * Creates a new tuple descriptor containing only the visible fields.
     */
    public RelationType withOnlyVisibleFields() {
        return new RelationType(visibleFields);
    }

    @Override
    public String toString() {
        return allFields.toString();
    }
}
