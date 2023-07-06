package io.github.melin.sqlflow.analyzer;

import io.github.melin.sqlflow.metadata.MetadataService;
import io.github.melin.sqlflow.tree.expression.Expression;
import io.github.melin.sqlflow.tree.expression.FunctionCall;
import io.github.melin.sqlflow.tree.expression.GroupingOperation;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.List;

import static io.github.melin.sqlflow.analyzer.ExpressionTreeUtils.*;
import static io.github.melin.sqlflow.analyzer.SemanticExceptions.semanticException;

public class Analyzer {

    static void verifyNoAggregateWindowOrGroupingFunctions(MetadataService metadataService, Expression predicate, String clause) {
        List<FunctionCall> aggregates = extractAggregateFunctions(ImmutableList.of(predicate), metadataService);

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
