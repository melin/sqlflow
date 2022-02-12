package com.github.melin.sqlflow.type;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * huaixin 2021/12/25 10:03 PM
 */
public class VarcharType implements Type {

    public static final int UNBOUNDED_LENGTH = Integer.MAX_VALUE;

    public static final int MAX_LENGTH = Integer.MAX_VALUE - 1;

    public static final VarcharType VARCHAR = new VarcharType(UNBOUNDED_LENGTH);

    private final int length;

    public static VarcharType createVarcharType(int length) {
        if (length > MAX_LENGTH || length < 0) {
            // Use createUnboundedVarcharType for unbounded VARCHAR.
            throw new IllegalArgumentException("Invalid VARCHAR length " + length);
        }
        return new VarcharType(length);
    }

    private VarcharType(int length) {
        if (length < 0) {
            throw new IllegalArgumentException("Invalid VARCHAR length " + length);
        }
        this.length = length;
    }

    public Optional<Integer> getLength() {
        if (isUnbounded()) {
            return Optional.empty();
        }
        return Optional.of(length);
    }

    public int getBoundedLength() {
        if (isUnbounded()) {
            throw new IllegalStateException("Cannot get size of unbounded VARCHAR.");
        }
        return length;
    }

    public boolean isUnbounded() {
        return length == UNBOUNDED_LENGTH;
    }

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
        return StandardTypes.VARCHAR;
    }
}
