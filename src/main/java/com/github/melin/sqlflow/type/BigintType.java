package com.github.melin.sqlflow.type;

import java.util.Collections;
import java.util.List;

/**
 * huaixin 2021/12/25 10:04 PM
 */
public class BigintType implements Type{
    public static final BigintType BIGINT = new BigintType();

    @Override
    public List<Type> getTypeParameters() {
        return Collections.emptyList();
    }

    @Override
    public String getDisplayName() {
        return toString();
    }

    @Override
    public boolean isComparable() {
        return true;
    }

    @Override
    public boolean isOrderable() {
        return true;
    }

    @Override
    public String toString() {
        return StandardTypes.BIGINT;
    }
}
