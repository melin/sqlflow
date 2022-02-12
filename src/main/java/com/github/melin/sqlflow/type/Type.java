package com.github.melin.sqlflow.type;

import java.util.List;

/**
 * huaixin 2021/12/25 10:58 AM
 */
public interface Type {

    /**
     * For parameterized types returns the list of parameters.
     */
    List<Type> getTypeParameters();

    /**
     * Returns the name of this type that should be displayed to end-users.
     */
    String getDisplayName();

    /**
     * True if the type supports equalTo and hash.
     */
    boolean isComparable();

    /**
     * True if the type supports compareTo.
     */
    boolean isOrderable();
}
