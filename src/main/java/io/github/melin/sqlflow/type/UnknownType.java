package io.github.melin.sqlflow.type;

import java.util.List;

/**
 * huaixin 2021/12/25 3:54 PM
 */
public class UnknownType implements Type {
    public static final UnknownType UNKNOWN = new UnknownType();

    public static final String NAME = "unknown";

    @Override
    public List<Type> getTypeParameters() {
        return null;
    }

    @Override
    public String getDisplayName() {
        return null;
    }

    @Override
    public boolean isComparable() {
        return true;
    }

    @Override
    public boolean isOrderable() {
        return true;
    }
}
