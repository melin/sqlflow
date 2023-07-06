package io.github.melin.sqlflow.analyzer;

import io.github.melin.sqlflow.tree.NodeRef;
import io.github.melin.sqlflow.tree.expression.*;
import io.github.melin.sqlflow.type.Type;
import io.github.melin.sqlflow.tree.expression.*;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/25 10:52 AM
 */
public class ExpressionAnalysis {
    private final Map<NodeRef<Expression>, Type> expressionTypes;
    private final Map<NodeRef<Expression>, Type> expressionCoercions;
    private final Set<NodeRef<Expression>> typeOnlyCoercions;
    private final Map<NodeRef<Expression>, ResolvedField> columnReferences;
    private final Set<NodeRef<InPredicate>> subqueryInPredicates;
    private final Set<NodeRef<SubqueryExpression>> subqueries;
    private final Set<NodeRef<ExistsPredicate>> existsSubqueries;
    private final Set<NodeRef<QuantifiedComparisonExpression>> quantifiedComparisons;
    private final Set<NodeRef<FunctionCall>> windowFunctions;

    public ExpressionAnalysis(
            Map<NodeRef<Expression>, Type> expressionTypes,
            Map<NodeRef<Expression>, Type> expressionCoercions,
            Set<NodeRef<InPredicate>> subqueryInPredicates,
            Set<NodeRef<SubqueryExpression>> subqueries,
            Set<NodeRef<ExistsPredicate>> existsSubqueries,
            Map<NodeRef<Expression>, ResolvedField> columnReferences,
            Set<NodeRef<Expression>> typeOnlyCoercions,
            Set<NodeRef<QuantifiedComparisonExpression>> quantifiedComparisons,
            Set<NodeRef<FunctionCall>> windowFunctions) {
        this.expressionTypes = ImmutableMap.copyOf(requireNonNull(expressionTypes, "expressionTypes is null"));
        this.expressionCoercions = ImmutableMap.copyOf(requireNonNull(expressionCoercions, "expressionCoercions is null"));
        this.typeOnlyCoercions = ImmutableSet.copyOf(requireNonNull(typeOnlyCoercions, "typeOnlyCoercions is null"));
        this.columnReferences = ImmutableMap.copyOf(requireNonNull(columnReferences, "columnReferences is null"));
        this.subqueryInPredicates = ImmutableSet.copyOf(requireNonNull(subqueryInPredicates, "subqueryInPredicates is null"));
        this.subqueries = ImmutableSet.copyOf(requireNonNull(subqueries, "subqueries is null"));
        this.existsSubqueries = ImmutableSet.copyOf(requireNonNull(existsSubqueries, "existsSubqueries is null"));
        this.quantifiedComparisons = ImmutableSet.copyOf(requireNonNull(quantifiedComparisons, "quantifiedComparisons is null"));
        this.windowFunctions = ImmutableSet.copyOf(requireNonNull(windowFunctions, "windowFunctions is null"));
    }

    public Type getType(Expression expression) {
        return expressionTypes.get(NodeRef.of(expression));
    }

    public Map<NodeRef<Expression>, Type> getExpressionTypes() {
        return expressionTypes;
    }

    public Type getCoercion(Expression expression) {
        return expressionCoercions.get(NodeRef.of(expression));
    }

    public boolean isTypeOnlyCoercion(Expression expression) {
        return typeOnlyCoercions.contains(NodeRef.of(expression));
    }

    public boolean isColumnReference(Expression node) {
        return columnReferences.containsKey(NodeRef.of(node));
    }

    public Map<NodeRef<Expression>, ResolvedField> getColumnReferences() {
        return columnReferences;
    }

    public Set<NodeRef<InPredicate>> getSubqueryInPredicates() {
        return subqueryInPredicates;
    }

    public Set<NodeRef<SubqueryExpression>> getSubqueries() {
        return subqueries;
    }

    public Set<NodeRef<ExistsPredicate>> getExistsSubqueries() {
        return existsSubqueries;
    }

    public Set<NodeRef<QuantifiedComparisonExpression>> getQuantifiedComparisons() {
        return quantifiedComparisons;
    }

    public Set<NodeRef<FunctionCall>> getWindowFunctions() {
        return windowFunctions;
    }
}
