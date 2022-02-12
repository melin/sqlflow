package com.github.melin.sqlflow.type;

import java.util.Collections;
import java.util.List;

/**
 * huaixin 2021/12/27 11:18 AM
 */
public class SmallIntType implements Type {

    public static final SmallIntType SMALLINT = new SmallIntType();

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
        return StandardTypes.SMALLINT;
    }

    public int getFixedSize()
    {
        return Short.BYTES;
    }
}
