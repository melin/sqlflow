package com.github.melin.sqlflow.type;

import java.util.Collections;
import java.util.List;

/**
 * huaixin 2021/12/27 11:18 AM
 */
public class TinyIntType implements Type {

    public static final TinyIntType TINYINT = new TinyIntType();

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
        return StandardTypes.TINYINT;
    }

    public int getFixedSize()
    {
        return Byte.BYTES;
    }
}
