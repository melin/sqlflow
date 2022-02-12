package com.github.melin.sqlflow.formatter;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.Node;
import com.github.melin.sqlflow.tree.window.rowPattern.*;
import com.github.melin.sqlflow.tree.window.rowPattern.*;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

/**
 * huaixin 2021/12/21 11:38 AM
 */
public final class RowPatternFormatter {
    private RowPatternFormatter() {
    }

    public static String formatPattern(RowPattern pattern) {
        return new Formatter().process(pattern, null);
    }

    public static class Formatter extends AstVisitor<String, Void> {
        @Override
        public String visitNode(Node node, Void context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String visitRowPattern(RowPattern node, Void context) {
            throw new UnsupportedOperationException(format("not yet implemented: %s.visit%s", getClass().getName(), node.getClass().getSimpleName()));
        }

        @Override
        public String visitPatternAlternation(PatternAlternation node, Void context) {
            return node.getPatterns().stream()
                    .map(child -> process(child, context))
                    .collect(joining(" | ", "(", ")"));
        }

        @Override
        public String visitPatternConcatenation(PatternConcatenation node, Void context) {
            return node.getPatterns().stream()
                    .map(child -> process(child, context))
                    .collect(joining(" ", "(", ")"));
        }

        @Override
        public String visitQuantifiedPattern(QuantifiedPattern node, Void context) {
            return "(" + process(node.getPattern(), context) + process(node.getPatternQuantifier(), context) + ")";
        }

        @Override
        public String visitPatternVariable(PatternVariable node, Void context) {
            return ExpressionFormatter.formatExpression(node.getName());
        }

        @Override
        public String visitEmptyPattern(EmptyPattern node, Void context) {
            return "()";
        }

        @Override
        public String visitPatternPermutation(PatternPermutation node, Void context) {
            return node.getPatterns().stream()
                    .map(child -> process(child, context))
                    .collect(joining(", ", "PERMUTE(", ")"));
        }

        @Override
        public String visitAnchorPattern(AnchorPattern node, Void context) {
            switch (node.getType()) {
                case PARTITION_START:
                    return "^";
                case PARTITION_END:
                    return "$";
                default:
                    throw new IllegalStateException("unexpected anchor pattern type: " + node.getType());
            }
        }

        @Override
        public String visitExcludedPattern(ExcludedPattern node, Void context) {
            return "{-" + process(node.getPattern(), context) + "-}";
        }

        @Override
        public String visitZeroOrMoreQuantifier(ZeroOrMoreQuantifier node, Void context) {
            String greedy = node.isGreedy() ? "" : "?";
            return "*" + greedy;
        }

        @Override
        public String visitOneOrMoreQuantifier(OneOrMoreQuantifier node, Void context) {
            String greedy = node.isGreedy() ? "" : "?";
            return "+" + greedy;
        }

        @Override
        public String visitZeroOrOneQuantifier(ZeroOrOneQuantifier node, Void context) {
            String greedy = node.isGreedy() ? "" : "?";
            return "?" + greedy;
        }

        @Override
        public String visitRangeQuantifier(RangeQuantifier node, Void context) {
            String greedy = node.isGreedy() ? "" : "?";
            String atLeast = node.getAtLeast().map(ExpressionFormatter::formatExpression).orElse("");
            String atMost = node.getAtMost().map(ExpressionFormatter::formatExpression).orElse("");
            return "{" + atLeast + "," + atMost + "}" + greedy;
        }
    }
}
