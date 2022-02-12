package com.github.melin.sqlflow.metadata;

import com.github.melin.sqlflow.type.Type;

import java.util.Objects;

import static com.github.melin.sqlflow.util.AstUtils.checkNotEmpty;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/25 4:34 PM
 */
public final class ColumnSchema {
    private final String name;
    private final Type type;
    private final boolean hidden;

    public ColumnSchema(String name, Type type, boolean hidden) {
        checkNotEmpty(name, "name");
        requireNonNull(type, "type is null");

        this.name = name.toLowerCase(ENGLISH);
        this.type = type;
        this.hidden = hidden;
    }

    public String getName() {
        return name;
    }

    public Type getType() {
        return type;
    }

    public boolean isHidden() {
        return hidden;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ColumnSchema that = (ColumnSchema) o;
        return hidden == that.hidden
                && name.equals(that.name)
                && type.equals(that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, hidden);
    }

    @Override
    public String toString() {
        return new StringBuilder("ColumnBasicMetadata{")
                .append("name='").append(name).append('\'')
                .append(", type=").append(type)
                .append(", hidden=").append(hidden)
                .append('}')
                .toString();
    }
}
