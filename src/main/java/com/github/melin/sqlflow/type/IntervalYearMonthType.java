package com.github.melin.sqlflow.type;

import java.util.Collections;
import java.util.List;

/**
 * huaixin 2021/12/27 10:50 AM
 */
public class IntervalYearMonthType implements Type {

    public static final IntervalYearMonthType INTERVAL_YEAR_MONTH = new IntervalYearMonthType();

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
        return StandardTypes.INTERVAL_YEAR_TO_MONTH;
    }
}
