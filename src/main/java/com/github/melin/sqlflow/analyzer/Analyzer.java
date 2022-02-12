package com.github.melin.sqlflow.analyzer;

import com.github.melin.sqlflow.metadata.Metadata;
import com.github.melin.sqlflow.tree.expression.Expression;
import com.github.melin.sqlflow.tree.expression.FunctionCall;
import com.github.melin.sqlflow.tree.expression.GroupingOperation;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.List;

import static com.github.melin.sqlflow.analyzer.ExpressionTreeUtils.*;
import static com.github.melin.sqlflow.analyzer.SemanticExceptions.semanticException;

public class Analyzer {

    static void verifyNoAggregateWindowOrGroupingFunctions(Metadata metadata, Expression predicate, String clause) {
        List<FunctionCall> aggregates = extractAggregateFunctions(ImmutableList.of(predicate), metadata);

        List<Expression> windowExpressions = extractWindowExpressions(ImmutableList.of(predicate));

        List<GroupingOperation> groupingOperations = extractExpressions(ImmutableList.of(predicate), GroupingOperation.class);

        List<Expression> found = ImmutableList.copyOf(Iterables.concat(
                aggregates,
                windowExpressions,
                groupingOperations));

        if (!found.isEmpty()) {
            throw semanticException(predicate, "%s cannot contain aggregations, window functions or grouping operations: %s", clause, found);
        }
    }
}
