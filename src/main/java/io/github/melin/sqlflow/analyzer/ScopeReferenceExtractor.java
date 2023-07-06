package io.github.melin.sqlflow.analyzer;

import io.github.melin.sqlflow.tree.Node;
import io.github.melin.sqlflow.tree.NodeRef;
import io.github.melin.sqlflow.tree.expression.Expression;
import io.github.melin.sqlflow.util.AstUtils;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * Extract expressions that are references to a given scope.
 */
final class ScopeReferenceExtractor {
    private ScopeReferenceExtractor() {
    }

    public static boolean hasReferencesToScope(Node node, Analysis analysis, Scope scope) {
        return getReferencesToScope(node, analysis, scope).findAny().isPresent();
    }

    public static Stream<Expression> getReferencesToScope(Node node, Analysis analysis, Scope scope) {
        Map<NodeRef<Expression>, ResolvedField> columnReferences = analysis.getColumnReferenceFields();

        return AstUtils.preOrder(node)
                .filter(Expression.class::isInstance)
                .map(Expression.class::cast)
                .filter(expression -> columnReferences.containsKey(NodeRef.of(expression)))
                .filter(expression -> isReferenceToScope(expression, scope, columnReferences));
    }

    private static boolean isReferenceToScope(Expression node, Scope scope, Map<NodeRef<Expression>, ResolvedField> columnReferences) {
        ResolvedField field = columnReferences.get(NodeRef.of(node));
        requireNonNull(field, () -> "No Field for " + node);
        return isFieldFromScope(field.getFieldId(), scope);
    }

    public static boolean isFieldFromScope(FieldId field, Scope scope) {
        return Objects.equals(field.getRelationId(), scope.getRelationId());
    }
}
