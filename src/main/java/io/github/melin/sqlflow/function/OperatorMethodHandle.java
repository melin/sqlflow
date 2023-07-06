package io.github.melin.sqlflow.function;

import java.lang.invoke.MethodHandle;

public class OperatorMethodHandle
{
    private final InvocationConvention callingConvention;
    private final MethodHandle methodHandle;

    public OperatorMethodHandle(InvocationConvention callingConvention, MethodHandle methodHandle)
    {
        this.callingConvention = callingConvention;
        this.methodHandle = methodHandle;
    }

    public InvocationConvention getCallingConvention()
    {
        return callingConvention;
    }

    public MethodHandle getMethodHandle()
    {
        return methodHandle;
    }
}
