package com.github.melin.sqlflow.analyzer;

import com.github.melin.sqlflow.DefaultExpressionTraversalVisitor;
import com.github.melin.sqlflow.metadata.Metadata;
import com.github.melin.sqlflow.tree.Node;
import com.github.melin.sqlflow.tree.NodeLocation;
import com.github.melin.sqlflow.tree.QualifiedName;
import com.github.melin.sqlflow.tree.expression.*;
import com.github.melin.sqlflow.tree.expression.*;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static java.util.Objects.requireNonNull;

public final class ExpressionTreeUtils {
    private ExpressionTreeUtils() {
    }

    static List<FunctionCall> extractAggregateFunctions(Iterable<? extends Node> nodes, Metadata metadata) {
        return extractExpressions(nodes, FunctionCall.class, function -> isAggregation(function, metadata));
    }

    static List<Expression> extractWindowExpressions(Iterable<? extends Node> nodes) {
        return ImmutableList.<Expression>builder()
                .addAll(extractWindowFunctions(nodes))
                .addAll(extractWindowMeasures(nodes))
                .build();
    }

    static List<FunctionCall> extractWindowFunctions(Iterable<? extends Node> nodes) {
        return extractExpressions(nodes, FunctionCall.class, ExpressionTreeUtils::isWindowFunction);
    }

    static List<WindowOperation> extractWindowMeasures(Iterable<? extends Node> nodes) {
        return extractExpressions(nodes, WindowOperation.class);
    }

    public static <T extends Expression> List<T> extractExpressions(
            Iterable<? extends Node> nodes,
            Class<T> clazz) {
        return extractExpressions(nodes, clazz, alwaysTrue());
    }

    private static boolean isAggregation(FunctionCall functionCall, Metadata metadata) {
        return ((metadata.isAggregationFunction(functionCall.getName()) || functionCall.getFilter().isPresent())
                && functionCall.getWindow() == null)
                || functionCall.getOrderBy().isPresent();
    }

    private static boolean isWindowFunction(FunctionCall functionCall) {
        return functionCall.getWindow().isPresent();
    }

    private static <T extends Expression> List<T> extractExpressions(
            Iterable<? extends Node> nodes,
            Class<T> clazz,
            Predicate<T> predicate) {
        requireNonNull(nodes, "nodes is null");
        requireNonNull(clazz, "clazz is null");
        requireNonNull(predicate, "predicate is null");

        return stream(nodes)
                .flatMap(node -> linearizeNodes(node).stream())
                .filter(clazz::isInstance)
                .map(clazz::cast)
                .filter(predicate)
                .collect(toImmutableList());
    }

    private static List<Node> linearizeNodes(Node node) {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        new DefaultExpressionTraversalVisitor<Void>() {
            @Override
            public Void process(Node node, Void context) {
                super.process(node, context);
                nodes.add(node);
                return null;
            }
        }.process(node, null);
        return nodes.build();
    }

    public static Optional<NodeLocation> extractLocation(Node node) {
        return node.getLocation()
                .map(location -> new NodeLocation(location.getLineNumber(), location.getColumnNumber(),
                        location.getStartIndex(), location.getStopIndex()));
    }

    public static QualifiedName asQualifiedName(Expression expression) {
        QualifiedName name = null;
        if (expression instanceof Identifier) {
            name = QualifiedName.of(((Identifier) expression).getValue());
        } else if (expression instanceof DereferenceExpression) {
            name = DereferenceExpression.getQualifiedName((DereferenceExpression) expression);
        }
        return name;
    }
}
