package io.github.melin.sqlflow.analyzer;

import io.github.melin.sqlflow.tree.NodeRef;
import io.github.melin.sqlflow.tree.expression.Expression;
import io.github.melin.sqlflow.tree.expression.FunctionCall;
import io.github.melin.sqlflow.tree.expression.Identifier;
import io.github.melin.sqlflow.tree.literal.LongLiteral;
import io.github.melin.sqlflow.tree.window.MeasureDefinition;
import io.github.melin.sqlflow.tree.window.SkipTo;
import io.github.melin.sqlflow.tree.window.SubsetDefinition;
import io.github.melin.sqlflow.tree.window.VariableDefinition;
import io.github.melin.sqlflow.tree.ProcessingMode;
import io.github.melin.sqlflow.tree.window.rowPattern.*;
import com.google.common.collect.*;
import io.github.melin.sqlflow.tree.window.rowPattern.*;

import java.util.*;

import static io.github.melin.sqlflow.analyzer.SemanticExceptions.semanticException;
import static io.github.melin.sqlflow.util.AstUtils.preOrder;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class PatternRecognitionAnalyzer {
    private PatternRecognitionAnalyzer() {
    }

    public static PatternRecognitionAnalysis analyze(
            List<SubsetDefinition> subsets,
            List<VariableDefinition> variableDefinitions,
            List<MeasureDefinition> measures,
            RowPattern pattern,
            Optional<SkipTo> skipTo) {
        // extract label names (Identifiers) from PATTERN and SUBSET clauses. create labels respecting SQL identifier semantics
        Set<String> primaryLabels = ExpressionTreeUtils.extractExpressions(ImmutableList.of(pattern), Identifier.class).stream()
                .map(PatternRecognitionAnalyzer::label)
                .collect(toImmutableSet());
        List<String> unionLabels = subsets.stream()
                .map(SubsetDefinition::getName)
                .map(PatternRecognitionAnalyzer::label)
                .collect(toImmutableList());

        // analyze SUBSET
        Set<String> unique = new HashSet<>();
        for (SubsetDefinition subset : subsets) {
            String label = label(subset.getName());
            if (primaryLabels.contains(label)) {
                throw semanticException(subset.getName(), "union pattern variable name: %s is a duplicate of primary pattern variable name", subset.getName());
            }
            if (!unique.add(label)) {
                throw semanticException(subset.getName(), "union pattern variable name: %s is declared twice", subset.getName());
            }
            for (Identifier element : subset.getIdentifiers()) {
                // TODO can there be repetitions in the list of subset elements? (currently repetitions are supported)
                if (!primaryLabels.contains(label(element))) {
                    throw SemanticExceptions.semanticException(element, "subset element: %s is not a primary pattern variable", element);
                }
            }
        }

        // analyze DEFINE
        unique = new HashSet<>();
        for (VariableDefinition definition : variableDefinitions) {
            String label = label(definition.getName());
            if (!primaryLabels.contains(label)) {
                throw semanticException(definition.getName(), "defined variable: %s is not a primary pattern variable", definition.getName());
            }
            if (!unique.add(label)) {
                throw semanticException(definition.getName(), "pattern variable with name: %s is defined twice", definition.getName());
            }
            // DEFINE clause only supports RUNNING semantics which is default
            Expression expression = definition.getExpression();
            ExpressionTreeUtils.extractExpressions(ImmutableList.of(expression), FunctionCall.class).stream()
                    .filter(functionCall -> functionCall.getProcessingMode().map(mode -> mode.getMode() == ProcessingMode.Mode.FINAL).orElse(false))
                    .findFirst()
                    .ifPresent(functionCall -> {
                        throw SemanticExceptions.semanticException(functionCall.getProcessingMode().get(), "FINAL semantics is not supported in DEFINE clause");
                    });
        }
        // record primary labels without definitions. they are implicitly associated with `true` condition
        Set<String> undefinedLabels = Sets.difference(primaryLabels, unique);

        // validate pattern quantifiers
        ImmutableMap.Builder<NodeRef<RangeQuantifier>, Analysis.Range> ranges = ImmutableMap.builder();
        preOrder(pattern)
                .filter(RangeQuantifier.class::isInstance)
                .map(RangeQuantifier.class::cast)
                .forEach(quantifier -> {
                    Optional<Long> atLeast = quantifier.getAtLeast().map(LongLiteral::getValue);
                    atLeast.ifPresent(value -> {
                        if (value < 0) {
                            throw SemanticExceptions.semanticException(quantifier, "Pattern quantifier lower bound must be greater than or equal to 0");
                        }
                        if (value > Integer.MAX_VALUE) {
                            throw SemanticExceptions.semanticException(quantifier, "Pattern quantifier lower bound must not exceed " + Integer.MAX_VALUE);
                        }
                    });
                    Optional<Long> atMost = quantifier.getAtMost().map(LongLiteral::getValue);
                    atMost.ifPresent(value -> {
                        if (value < 1) {
                            throw SemanticExceptions.semanticException(quantifier, "Pattern quantifier upper bound must be greater than or equal to 1");
                        }
                        if (value > Integer.MAX_VALUE) {
                            throw SemanticExceptions.semanticException(quantifier, "Pattern quantifier upper bound must not exceed " + Integer.MAX_VALUE);
                        }
                    });
                    if (atLeast.isPresent() && atMost.isPresent()) {
                        if (atLeast.get() > atMost.get()) {
                            throw SemanticExceptions.semanticException(quantifier, "Pattern quantifier lower bound must not exceed upper bound");
                        }
                    }
                    ranges.put(NodeRef.of(quantifier), new Analysis.Range(atLeast.map(Math::toIntExact), atMost.map(Math::toIntExact)));
                });

        // validate AFTER MATCH SKIP
        Set<String> allLabels = ImmutableSet.<String>builder()
                .addAll(primaryLabels)
                .addAll(unionLabels)
                .build();
        skipTo.flatMap(SkipTo::getIdentifier)
                .ifPresent(identifier -> {
                    String label = label(identifier);
                    if (!allLabels.contains(label)) {
                        throw semanticException(identifier, "%s is not a primary or union pattern variable", identifier);
                    }
                });

        // check no prohibited nesting: cannot nest one row pattern recognition within another
        List<Expression> expressions = Streams.concat(
                        measures.stream()
                                .map(MeasureDefinition::getExpression),
                        variableDefinitions.stream()
                                .map(VariableDefinition::getExpression))
                .collect(toImmutableList());
        expressions.forEach(expression -> preOrder(expression)
                .filter(child -> child instanceof PatternRecognitionRelation || child instanceof RowPattern)
                .findFirst()
                .ifPresent(nested -> {
                    throw SemanticExceptions.semanticException(nested, "nested row pattern recognition in row pattern recognition");
                }));

        return new PatternRecognitionAnalysis(allLabels, undefinedLabels, ranges.build());
    }

    public static void validateNoPatternSearchMode(Optional<PatternSearchMode> patternSearchMode) {
        patternSearchMode.ifPresent(mode -> {
            throw semanticException(mode, "Pattern search modifier: %s is not allowed in MATCH_RECOGNIZE clause", mode.getMode());
        });
    }

    public static void validatePatternExclusions(Optional<PatternRecognitionRelation.RowsPerMatch> rowsPerMatch, RowPattern pattern) {
        // exclusion syntax is not allowed in row pattern if ALL ROWS PER MATCH WITH UNMATCHED ROWS is specified
        if (rowsPerMatch.isPresent() && rowsPerMatch.get().isUnmatchedRows()) {
            preOrder(pattern)
                    .filter(ExcludedPattern.class::isInstance)
                    .findFirst()
                    .ifPresent(exclusion -> {
                        throw SemanticExceptions.semanticException(exclusion, "Pattern exclusion syntax is not allowed when ALL ROWS PER MATCH WITH UNMATCHED ROWS is specified");
                    });
        }
    }

    public static void validateNoPatternAnchors(RowPattern pattern) {
        preOrder(pattern)
                .filter(AnchorPattern.class::isInstance)
                .findFirst()
                .ifPresent(anchor -> {
                    throw SemanticExceptions.semanticException(anchor, "Anchor pattern syntax is not allowed in window");
                });
    }

    public static void validateNoMatchNumber(List<MeasureDefinition> measures, List<VariableDefinition> variableDefinitions, Set<NodeRef<FunctionCall>> patternRecognitionFunctions) {
        List<Expression> expressions = Streams.concat(
                        measures.stream()
                                .map(MeasureDefinition::getExpression),
                        variableDefinitions.stream()
                                .map(VariableDefinition::getExpression))
                .collect(toImmutableList());
        expressions.forEach(expression -> preOrder(expression)
                .filter(child -> patternRecognitionFunctions.contains(NodeRef.of(child)))
                .filter(child -> ((FunctionCall) child).getName().getSuffix().equalsIgnoreCase("MATCH_NUMBER"))
                .findFirst()
                .ifPresent(matchNumber -> {
                    throw SemanticExceptions.semanticException(matchNumber, "MATCH_NUMBER function is not supported in window");
                }));
    }

    private static String label(Identifier identifier) {
        return identifier.getCanonicalValue();
    }

    public static class PatternRecognitionAnalysis {
        private final Set<String> allLabels;
        private final Set<String> undefinedLabels;
        private final Map<NodeRef<RangeQuantifier>, Analysis.Range> ranges;

        public PatternRecognitionAnalysis(Set<String> allLabels, Set<String> undefinedLabels, Map<NodeRef<RangeQuantifier>, Analysis.Range> ranges) {
            this.allLabels = requireNonNull(allLabels, "allLabels is null");
            this.undefinedLabels = ImmutableSet.copyOf(requireNonNull(undefinedLabels, "undefinedLabels is null"));
            this.ranges = ImmutableMap.copyOf(requireNonNull(ranges, "ranges is null"));
        }

        public Set<String> getAllLabels() {
            return allLabels;
        }

        public Set<String> getUndefinedLabels() {
            return undefinedLabels;
        }

        public Map<NodeRef<RangeQuantifier>, Analysis.Range> getRanges() {
            return ranges;
        }
    }
}
