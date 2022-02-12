package com.github.melin.sqlflow.type;

import java.util.Collections;
import java.util.List;

/**
 * huaixin 2021/12/27 10:44 AM
 */
public class IntervalDayTimeType implements Type {
    public static final IntervalDayTimeType INTERVAL_DAY_TIME = new IntervalDayTimeType();

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
        return StandardTypes.INTERVAL_DAY_TO_SECOND;
    }
}
