package io.github.melin.sqlflow.metadata;

import io.github.melin.sqlflow.type.Type;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/25 5:36 PM
 */
public final class ViewColumn {
    private final String name;
    private final Type type;

    public ViewColumn(String name, Type type) {
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
    }

    public String getName() {
        return name;
    }

    public Type getType() {
        return type;
    }

    @Override
    public String toString() {
        return name + " " + type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ViewColumn that = (ViewColumn) o;
        return Objects.equals(name, that.name) && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type);
    }
}
