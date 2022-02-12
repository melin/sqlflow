package com.github.melin.sqlflow.function;

import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class InvocationConvention {
    private final List<InvocationArgumentConvention> argumentConventionList;
    private final InvocationReturnConvention returnConvention;
    private final boolean supportsSession;
    private final boolean supportsInstanceFactory;

    public static InvocationConvention simpleConvention(InvocationReturnConvention returnConvention, InvocationArgumentConvention... argumentConventions) {
        return new InvocationConvention(Arrays.asList(argumentConventions), returnConvention, false, false);
    }

    public InvocationConvention(
            List<InvocationArgumentConvention> argumentConventionList,
            InvocationReturnConvention returnConvention,
            boolean supportsSession,
            boolean supportsInstanceFactory) {
        this.argumentConventionList = Lists.newArrayList(requireNonNull(argumentConventionList, "argumentConventionList is null"));
        this.returnConvention = returnConvention;
        this.supportsSession = supportsSession;
        this.supportsInstanceFactory = supportsInstanceFactory;
    }

    public InvocationReturnConvention getReturnConvention() {
        return returnConvention;
    }

    public List<InvocationArgumentConvention> getArgumentConventions() {
        return argumentConventionList;
    }

    public InvocationArgumentConvention getArgumentConvention(int index) {
        return argumentConventionList.get(index);
    }

    public boolean supportsSession() {
        return supportsSession;
    }

    public boolean supportsInstanceFactor() {
        return supportsInstanceFactory;
    }

    @Override
    public String toString() {
        return "(" + argumentConventionList.toString() + ")" + returnConvention;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InvocationConvention that = (InvocationConvention) o;
        return supportsSession == that.supportsSession &&
                supportsInstanceFactory == that.supportsInstanceFactory &&
                Objects.equals(argumentConventionList, that.argumentConventionList) &&
                returnConvention == that.returnConvention;
    }

    @Override
    public int hashCode() {
        return Objects.hash(argumentConventionList, returnConvention, supportsSession, supportsInstanceFactory);
    }

    public enum InvocationArgumentConvention {
        /**
         * Argument must not be a boxed type. Argument will never be null.
         */
        NEVER_NULL(false, 1),
        /**
         * Argument is always an object type. A SQL null will be passed a Java null.
         */
        BOXED_NULLABLE(true, 1),
        /**
         * Argument must not be a boxed type, and is always followed with a boolean argument
         * to indicate if the sql value is null.
         */
        NULL_FLAG(true, 2),
        /**
         * Argument is passed a Block followed by the integer position in the block.  The
         * sql value may be null.
         */
        BLOCK_POSITION(true, 2),
        /**
         * Argument is a lambda function.
         */
        FUNCTION(false, 1);

        private final boolean nullable;
        private final int parameterCount;

        InvocationArgumentConvention(boolean nullable, int parameterCount) {
            this.nullable = nullable;
            this.parameterCount = parameterCount;
        }

        public boolean isNullable() {
            return nullable;
        }

        public int getParameterCount() {
            return parameterCount;
        }
    }

    public enum InvocationReturnConvention {
        FAIL_ON_NULL(false),
        NULLABLE_RETURN(true);

        private final boolean nullable;

        InvocationReturnConvention(boolean nullable) {
            this.nullable = nullable;
        }

        public boolean isNullable() {
            return nullable;
        }
    }
}
