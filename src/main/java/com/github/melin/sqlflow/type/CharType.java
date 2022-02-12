package com.github.melin.sqlflow.type;

import com.github.melin.sqlflow.SqlFlowException;

import java.util.Collections;
import java.util.List;

import static java.lang.String.format;

/**
 * huaixin 2021/12/27 11:22 AM
 */
public class CharType implements Type {

    public static final int MAX_LENGTH = 65_536;

    private final int length;

    public static CharType createCharType(long length) {
        return new CharType(length);
    }

    private CharType(long length) {
        if (length < 0 || length > MAX_LENGTH) {
            throw new SqlFlowException(format("CHAR length must be in range [0, %s], got %s", MAX_LENGTH, length));
        }
        this.length = (int) length;
    }

    public int getLength() {
        return length;
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
        return "char(" + length + ')';
    }
}
