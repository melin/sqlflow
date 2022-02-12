package com.github.melin.sqlflow.analyzer;

import com.github.melin.sqlflow.SqlFlowException;
import com.github.melin.sqlflow.tree.Node;
import com.github.melin.sqlflow.tree.QualifiedName;
import com.github.melin.sqlflow.tree.expression.Expression;

import static com.github.melin.sqlflow.analyzer.ExpressionTreeUtils.extractLocation;
import static java.lang.String.format;

/**
 * huaixin 2021/12/24 11:34 AM
 */
public class SemanticExceptions {

    private SemanticExceptions() {
    }

    public static SqlFlowException missingAttributeException(Expression node, QualifiedName name) {
        throw semanticException(node, "Column '%s' cannot be resolved", name);
    }

    public static SqlFlowException ambiguousAttributeException(Expression node, QualifiedName name) {
        throw semanticException(node, "Column '%s' is ambiguous", name);
    }

    public static SqlFlowException semanticException(Node node, String format, Object... args) {
        return semanticException(node, null, format, args);
    }

    public static SqlFlowException semanticException(Node node, Throwable cause, String format, Object... args) {
        throw new SqlFlowException(extractLocation(node), format(format, args), cause);
    }
}
