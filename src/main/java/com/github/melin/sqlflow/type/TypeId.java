package com.github.melin.sqlflow.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Represents an opaque identifier for a Type than can be used
 * for serialization or storage in external systems.
 */
public class TypeId {
    private final String id;

    private TypeId(String id) {
        this.id = requireNonNull(id, "id is null");
    }

    @JsonCreator
    public static TypeId of(String id) {
        return new TypeId(id);
    }

    @JsonValue
    public String getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TypeId typeId = (TypeId) o;
        return id.equals(typeId.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "type:[" + getId() + "]";
    }
}
