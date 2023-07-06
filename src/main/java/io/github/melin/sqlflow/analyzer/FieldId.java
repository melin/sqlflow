package io.github.melin.sqlflow.analyzer;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class FieldId {
    public static FieldId from(ResolvedField field) {
        requireNonNull(field, "field is null");

        Scope sourceScope = field.getScope();
        RelationType relationType = sourceScope.getRelationType();
        return new FieldId(sourceScope.getRelationId(), relationType.indexOf(field.getField()));
    }

    private final RelationId relationId;
    private final int fieldIndex;

    public FieldId(RelationId relationId, int fieldIndex) {
        this.relationId = requireNonNull(relationId, "relationId is null");

        checkArgument(fieldIndex >= 0, "fieldIndex must be non-negative, got: %s", fieldIndex);
        this.fieldIndex = fieldIndex;
    }

    public RelationId getRelationId() {
        return relationId;
    }

    /**
     * Returns {@link RelationType#indexOf(Field) field index} of the field in the containing relation.
     */
    public int getFieldIndex() {
        return fieldIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FieldId fieldId = (FieldId) o;
        return fieldIndex == fieldId.fieldIndex &&
                Objects.equals(relationId, fieldId.relationId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(relationId, fieldIndex);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .addValue(relationId)
                .addValue(fieldIndex)
                .toString();
    }
}
