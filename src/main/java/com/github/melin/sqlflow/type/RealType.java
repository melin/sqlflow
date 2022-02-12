package com.github.melin.sqlflow.type;

import java.util.Collections;
import java.util.List;

/**
 * huaixin 2021/12/27 1:18 PM
 */
public class RealType implements Type {

    public static final RealType REAL = new RealType();

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
        return StandardTypes.REAL;
    }
}
