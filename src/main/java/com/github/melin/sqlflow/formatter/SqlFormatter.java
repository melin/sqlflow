package com.github.melin.sqlflow.formatter;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.*;
import com.github.melin.sqlflow.tree.expression.*;
import com.github.melin.sqlflow.tree.filter.*;
import com.github.melin.sqlflow.tree.join.*;
import com.github.melin.sqlflow.tree.merge.*;
import com.github.melin.sqlflow.tree.relation.*;
import com.github.melin.sqlflow.tree.statement.*;
import com.github.melin.sqlflow.tree.window.rowPattern.*;
import com.github.melin.sqlflow.tree.*;
import com.github.melin.sqlflow.tree.expression.Expression;
import com.github.melin.sqlflow.tree.expression.Identifier;
import com.github.melin.sqlflow.tree.expression.Row;
import com.github.melin.sqlflow.tree.filter.FetchFirst;
import com.github.melin.sqlflow.tree.filter.Limit;
import com.github.melin.sqlflow.tree.filter.Offset;
import com.github.melin.sqlflow.tree.join.Join;
import com.github.melin.sqlflow.tree.join.JoinCriteria;
import com.github.melin.sqlflow.tree.join.JoinOn;
import com.github.melin.sqlflow.tree.join.JoinUsing;
import com.github.melin.sqlflow.tree.merge.MergeCase;
import com.github.melin.sqlflow.tree.merge.MergeDelete;
import com.github.melin.sqlflow.tree.merge.MergeInsert;
import com.github.melin.sqlflow.tree.merge.MergeUpdate;
import com.github.melin.sqlflow.tree.relation.*;
import com.github.melin.sqlflow.tree.statement.*;
import com.github.melin.sqlflow.tree.window.rowPattern.PatternRecognitionRelation;
import com.github.melin.sqlflow.tree.window.rowPattern.RowPattern;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterables.transform;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

/**
 * huaixin 2021/12/19 6:03 PM
 */
public final class SqlFormatter {
    private static final String INDENT = "   ";

    private SqlFormatter() {
    }

    public static String formatSql(Node root) {
        StringBuilder builder = new StringBuilder();
        new Formatter(builder).process(root, 0);
        return builder.toString();
    }

    static String formatName(QualifiedName name) {
        return name.getOriginalParts().stream()
                .map(ExpressionFormatter::formatExpression)
                .collect(joining("."));
    }

    private static class Formatter extends AstVisitor<Void, Integer> {
        private final StringBuilder builder;

        public Formatter(StringBuilder builder) {
            this.builder = builder;
        }

        @Override
        public Void visitNode(Node node, Integer indent) {
            throw new UnsupportedOperationException("not yet implemented: " + node);
        }

        @Override
        public Void visitExpression(Expression node, Integer indent) {
            checkArgument(indent == 0, "visitExpression should only be called at root");
            builder.append(ExpressionFormatter.formatExpression(node));
            return null;
        }

        @Override
        public Void visitRowPattern(RowPattern node, Integer indent) {
            checkArgument(indent == 0, "visitRowPattern should only be called at root");
            builder.append(RowPatternFormatter.formatPattern(node));
            return null;
        }

        @Override
        public Void visitUnnest(Unnest node, Integer indent) {
            builder.append("UNNEST(")
                    .append(node.getExpressions().stream()
                            .map(ExpressionFormatter::formatExpression)
                            .collect(joining(", ")))
                    .append(")");
            if (node.isWithOrdinality()) {
                builder.append(" WITH ORDINALITY");
            }
            return null;
        }

        @Override
        public Void visitLateral(Lateral node, Integer indent) {
            append(indent, "LATERAL (");
            process(node.getQuery(), indent + 1);
            append(indent, ")");
            return null;
        }

        @Override
        public Void visitQuery(Query node, Integer indent) {
            node.getWith().ifPresent(with -> {
                append(indent, "WITH");
                if (with.isRecursive()) {
                    builder.append(" RECURSIVE");
                }
                builder.append("\n  ");
                Iterator<WithQuery> queries = with.getQueries().iterator();
                while (queries.hasNext()) {
                    WithQuery query = queries.next();
                    append(indent, ExpressionFormatter.formatExpression(query.getName()));
                    query.getColumnNames().ifPresent(columnNames -> appendAliasColumns(builder, columnNames));
                    builder.append(" AS ");
                    process(new TableSubquery(query.getQuery()), indent);
                    builder.append('\n');
                    if (queries.hasNext()) {
                        builder.append(", ");
                    }
                }
            });

            processRelation(node.getQueryBody(), indent);
            node.getOrderBy().ifPresent(orderBy -> process(orderBy, indent));
            node.getOffset().ifPresent(offset -> process(offset, indent));
            node.getLimit().ifPresent(limit -> process(limit, indent));
            return null;
        }

        @Override
        public Void visitQuerySpecification(QuerySpecification node, Integer indent) {
            process(node.getSelect(), indent);

            node.getFrom().ifPresent(from -> {
                append(indent, "FROM");
                builder.append('\n');
                append(indent, "  ");
                process(from, indent);
            });

            builder.append('\n');

            node.getWhere().ifPresent(where ->
                    append(indent, "WHERE " + ExpressionFormatter.formatExpression(where)).append('\n'));

            node.getGroupBy().ifPresent(groupBy ->
                    append(indent, "GROUP BY " + (groupBy.isDistinct() ? " DISTINCT " : "") + ExpressionFormatter.formatGroupBy(groupBy.getGroupingElements())).append('\n'));

            node.getHaving().ifPresent(having -> append(indent, "HAVING " + ExpressionFormatter.formatExpression(having))
                    .append('\n'));

            if (!node.getWindows().isEmpty()) {
                append(indent, "WINDOW");
                formatDefinitionList(node.getWindows().stream()
                        .map(definition -> ExpressionFormatter.formatExpression(definition.getName()) + " AS " + ExpressionFormatter.formatWindowSpecification(definition.getWindow()))
                        .collect(toImmutableList()), indent + 1);
            }

            node.getOrderBy().ifPresent(orderBy -> process(orderBy, indent));
            node.getOffset().ifPresent(offset -> process(offset, indent));
            node.getLimit().ifPresent(limit -> process(limit, indent));
            return null;
        }

        @Override
        public Void visitOrderBy(OrderBy node, Integer indent) {
            append(indent, ExpressionFormatter.formatOrderBy(node))
                    .append('\n');
            return null;
        }

        @Override
        public Void visitOffset(Offset node, Integer indent) {
            append(indent, "OFFSET ")
                    .append(ExpressionFormatter.formatExpression(node.getRowCount()))
                    .append(" ROWS\n");
            return null;
        }

        @Override
        public Void visitFetchFirst(FetchFirst node, Integer indent) {
            append(indent, "FETCH FIRST " + node.getRowCount().map(count -> ExpressionFormatter.formatExpression(count) + " ROWS ").orElse("ROW "))
                    .append(node.isWithTies() ? "WITH TIES" : "ONLY")
                    .append('\n');
            return null;
        }

        @Override
        public Void visitLimit(Limit node, Integer indent) {
            append(indent, "LIMIT ")
                    .append(ExpressionFormatter.formatExpression(node.getRowCount()))
                    .append('\n');
            return null;
        }

        @Override
        public Void visitSelect(Select node, Integer indent) {
            append(indent, "SELECT");
            if (node.isDistinct()) {
                builder.append(" DISTINCT");
            }

            if (node.getSelectItems().size() > 1) {
                boolean first = true;
                for (SelectItem item : node.getSelectItems()) {
                    builder.append("\n")
                            .append(indentString(indent))
                            .append(first ? "  " : ", ");

                    process(item, indent);
                    first = false;
                }
            } else {
                builder.append(' ');
                process(getOnlyElement(node.getSelectItems()), indent);
            }

            builder.append('\n');

            return null;
        }

        @Override
        public Void visitSingleColumn(SingleColumn node, Integer indent) {
            builder.append(ExpressionFormatter.formatExpression(node.getExpression()));
            node.getAlias().ifPresent(alias -> builder
                    .append(' ')
                    .append(ExpressionFormatter.formatExpression(alias)));

            return null;
        }

        @Override
        public Void visitAllColumns(AllColumns node, Integer indent) {
            node.getTarget().ifPresent(value -> builder
                    .append(ExpressionFormatter.formatExpression(value))
                    .append("."));
            builder.append("*");

            if (!node.getAliases().isEmpty()) {
                builder.append(" AS (")
                        .append(Joiner.on(", ").join(node.getAliases().stream()
                                .map(ExpressionFormatter::formatExpression)
                                .collect(toImmutableList())))
                        .append(")");
            }

            return null;
        }

        @Override
        public Void visitTable(Table node, Integer indent) {
            builder.append(formatName(node.getName()));
            node.getQueryPeriod().ifPresent(queryPeriod -> builder
                    .append(" " + queryPeriod));
            return null;
        }

        @Override
        public Void visitQueryPeriod(QueryPeriod node, Integer indent) {
            builder.append("FOR " + node.getRangeType().name() + " AS OF " + ExpressionFormatter.formatExpression(node.getEnd().get()));
            return null;
        }

        @Override
        public Void visitJoin(Join node, Integer indent) {
            JoinCriteria criteria = node.getCriteria().orElse(null);
            String type = node.getType().toString();
            if (criteria instanceof NaturalJoin) {
                type = "NATURAL " + type;
            }

            if (node.getType() != Join.Type.IMPLICIT) {
                builder.append('(');
            }
            process(node.getLeft(), indent);

            builder.append('\n');
            if (node.getType() == Join.Type.IMPLICIT) {
                append(indent, ", ");
            } else {
                append(indent, type).append(" JOIN ");
            }

            process(node.getRight(), indent);

            if (node.getType() != Join.Type.CROSS && node.getType() != Join.Type.IMPLICIT) {
                if (criteria instanceof JoinUsing) {
                    JoinUsing using = (JoinUsing) criteria;
                    builder.append(" USING (")
                            .append(Joiner.on(", ").join(using.getColumns()))
                            .append(")");
                } else if (criteria instanceof JoinOn) {
                    JoinOn on = (JoinOn) criteria;
                    builder.append(" ON ")
                            .append(ExpressionFormatter.formatExpression(on.getExpression()));
                } else if (!(criteria instanceof NaturalJoin)) {
                    throw new UnsupportedOperationException("unknown join criteria: " + criteria);
                }
            }

            if (node.getType() != Join.Type.IMPLICIT) {
                builder.append(")");
            }

            return null;
        }

        @Override
        public Void visitAliasedRelation(AliasedRelation node, Integer indent) {
            processRelationSuffix(node.getRelation(), indent);

            builder.append(' ')
                    .append(ExpressionFormatter.formatExpression(node.getAlias()));
            appendAliasColumns(builder, node.getColumnNames());

            return null;
        }

        @Override
        public Void visitPatternRecognitionRelation(PatternRecognitionRelation node, Integer indent) {
            processRelationSuffix(node.getInput(), indent);

            builder.append(" MATCH_RECOGNIZE (\n");
            if (!node.getPartitionBy().isEmpty()) {
                append(indent + 1, "PARTITION BY ")
                        .append(node.getPartitionBy().stream()
                                .map(ExpressionFormatter::formatExpression)
                                .collect(joining(", ")))
                        .append("\n");
            }
            node.getOrderBy().ifPresent(orderBy -> process(orderBy, indent + 1));

            if (!node.getMeasures().isEmpty()) {
                append(indent + 1, "MEASURES");
                formatDefinitionList(node.getMeasures().stream()
                        .map(measure -> ExpressionFormatter.formatExpression(measure.getExpression()) + " AS " + ExpressionFormatter.formatExpression(measure.getName()))
                        .collect(toImmutableList()), indent + 2);
            }

            node.getRowsPerMatch().ifPresent(rowsPerMatch -> {
                String rowsPerMatchDescription;
                switch (rowsPerMatch) {
                    case ONE:
                        rowsPerMatchDescription = "ONE ROW PER MATCH";
                        break;
                    case ALL_SHOW_EMPTY:
                        rowsPerMatchDescription = "ALL ROWS PER MATCH SHOW EMPTY MATCHES";
                        break;
                    case ALL_OMIT_EMPTY:
                        rowsPerMatchDescription = "ALL ROWS PER MATCH OMIT EMPTY MATCHES";
                        break;
                    case ALL_WITH_UNMATCHED:
                        rowsPerMatchDescription = "ALL ROWS PER MATCH WITH UNMATCHED ROWS";
                        break;
                    default:
                        // RowsPerMatch of type WINDOW cannot occur in MATCH_RECOGNIZE clause
                        throw new IllegalStateException("unexpected rowsPerMatch: " + node.getRowsPerMatch().get());
                }
                append(indent + 1, rowsPerMatchDescription)
                        .append("\n");
            });

            node.getAfterMatchSkipTo().ifPresent(afterMatchSkipTo -> {
                String skipTo = ExpressionFormatter.formatSkipTo(afterMatchSkipTo);
                append(indent + 1, skipTo)
                        .append("\n");
            });

            node.getPatternSearchMode().ifPresent(patternSearchMode ->
                    append(indent + 1, patternSearchMode.getMode().name())
                            .append("\n"));

            append(indent + 1, "PATTERN (")
                    .append(RowPatternFormatter.formatPattern(node.getPattern()))
                    .append(")\n");
            if (!node.getSubsets().isEmpty()) {
                append(indent + 1, "SUBSET");
                formatDefinitionList(node.getSubsets().stream()
                        .map(subset -> ExpressionFormatter.formatExpression(subset.getName()) + " = " + subset.getIdentifiers().stream()
                                .map(ExpressionFormatter::formatExpression).collect(joining(", ", "(", ")")))
                        .collect(toImmutableList()), indent + 2);
            }
            append(indent + 1, "DEFINE");
            formatDefinitionList(node.getVariableDefinitions().stream()
                    .map(variable -> ExpressionFormatter.formatExpression(variable.getName()) + " AS " + ExpressionFormatter.formatExpression(variable.getExpression()))
                    .collect(toImmutableList()), indent + 2);

            builder.append(")");

            return null;
        }

        @Override
        public Void visitSampledRelation(SampledRelation node, Integer indent) {
            processRelationSuffix(node.getRelation(), indent);

            builder.append(" TABLESAMPLE ")
                    .append(node.getType())
                    .append(" (")
                    .append(node.getSamplePercentage())
                    .append(')');

            return null;
        }

        private void processRelationSuffix(Relation relation, Integer indent) {
            if ((relation instanceof AliasedRelation) || (relation instanceof SampledRelation) || (relation instanceof PatternRecognitionRelation)) {
                builder.append("( ");
                process(relation, indent + 1);
                append(indent, ")");
            } else {
                process(relation, indent);
            }
        }

        @Override
        public Void visitValues(Values node, Integer indent) {
            builder.append(" VALUES ");

            boolean first = true;
            for (Expression row : node.getRows()) {
                builder.append("\n")
                        .append(indentString(indent))
                        .append(first ? "  " : ", ");

                builder.append(ExpressionFormatter.formatExpression(row));
                first = false;
            }
            builder.append('\n');

            return null;
        }

        @Override
        public Void visitTableSubquery(TableSubquery node, Integer indent) {
            builder.append('(')
                    .append('\n');

            process(node.getQuery(), indent + 1);

            append(indent, ") ");

            return null;
        }

        @Override
        public Void visitUnion(Union node, Integer indent) {
            Iterator<Relation> relations = node.getRelations().iterator();

            while (relations.hasNext()) {
                processRelation(relations.next(), indent);

                if (relations.hasNext()) {
                    builder.append("UNION ");
                    if (!node.isDistinct()) {
                        builder.append("ALL ");
                    }
                }
            }

            return null;
        }

        @Override
        public Void visitExcept(Except node, Integer indent) {
            processRelation(node.getLeft(), indent);

            builder.append("EXCEPT ");
            if (!node.isDistinct()) {
                builder.append("ALL ");
            }

            processRelation(node.getRight(), indent);

            return null;
        }

        @Override
        public Void visitIntersect(Intersect node, Integer indent) {
            Iterator<Relation> relations = node.getRelations().iterator();

            while (relations.hasNext()) {
                processRelation(relations.next(), indent);

                if (relations.hasNext()) {
                    builder.append("INTERSECT ");
                    if (!node.isDistinct()) {
                        builder.append("ALL ");
                    }
                }
            }

            return null;
        }

        @Override
        public Void visitMerge(Merge node, Integer indent) {
            builder.append("MERGE INTO ")
                    .append(node.getTable().getName());

            node.getTargetAlias().ifPresent(value -> builder
                    .append(' ')
                    .append(value));
            builder.append("\n");

            append(indent + 1, "USING ");

            processRelation(node.getRelation(), indent + 2);

            builder.append("\n");
            append(indent + 1, "ON ");
            builder.append(ExpressionFormatter.formatExpression(node.getExpression()));

            for (MergeCase mergeCase : node.getMergeCases()) {
                builder.append("\n");
                process(mergeCase, indent);
            }

            return null;
        }

        @Override
        public Void visitMergeInsert(MergeInsert node, Integer indent) {
            appendMergeCaseWhen(false, node.getExpression());
            append(indent + 1, "THEN INSERT ");

            if (!node.getColumns().isEmpty()) {
                builder.append("(");
                Joiner.on(", ").appendTo(builder, node.getColumns());
                builder.append(")");
            }

            builder.append("VALUES (");
            Joiner.on(", ").appendTo(builder, transform(node.getValues(), ExpressionFormatter::formatExpression));
            builder.append(")");

            return null;
        }

        @Override
        public Void visitMergeUpdate(MergeUpdate node, Integer indent) {
            appendMergeCaseWhen(true, node.getExpression());
            append(indent + 1, "THEN UPDATE SET");

            boolean first = true;
            for (MergeUpdate.Assignment assignment : node.getAssignments()) {
                builder.append("\n");
                append(indent + 1, first ? "  " : ", ");
                builder.append(assignment.getTarget())
                        .append(" = ")
                        .append(ExpressionFormatter.formatExpression(assignment.getValue()));
                first = false;
            }

            return null;
        }

        @Override
        public Void visitMergeDelete(MergeDelete node, Integer indent) {
            appendMergeCaseWhen(true, node.getExpression());
            append(indent + 1, "THEN DELETE");
            return null;
        }

        private void appendMergeCaseWhen(boolean matched, Optional<Expression> expression) {
            builder.append(matched ? "WHEN MATCHED" : "WHEN NOT MATCHED");
            expression.ifPresent(value -> builder
                    .append(" AND ")
                    .append(ExpressionFormatter.formatExpression(value)));
            builder.append("\n");
        }

        @Override
        public Void visitCreateView(CreateView node, Integer indent) {
            builder.append("CREATE ");
            if (node.isReplace()) {
                builder.append("OR REPLACE ");
            }
            builder.append("VIEW ")
                    .append(formatName(node.getName()));

            node.getComment().ifPresent(comment -> builder
                    .append(" COMMENT ")
                    .append(ExpressionFormatter.formatStringLiteral(comment)));

            node.getSecurity().ifPresent(security -> builder
                    .append(" SECURITY ")
                    .append(security));

            builder.append(" AS\n");

            process(node.getQuery(), indent);

            return null;
        }

        @Override
        public Void visitCreateMaterializedView(CreateMaterializedView node, Integer indent) {
            builder.append("CREATE ");
            if (node.isReplace()) {
                builder.append("OR REPLACE ");
            }
            builder.append("MATERIALIZED VIEW ");

            if (node.isNotExists()) {
                builder.append("IF NOT EXISTS ");
            }

            builder.append(formatName(node.getName()));
            node.getComment().ifPresent(comment -> builder
                    .append("\nCOMMENT ")
                    .append(ExpressionFormatter.formatStringLiteral(comment)));
            builder.append(formatPropertiesMultiLine(node.getProperties()));
            builder.append(" AS\n");

            process(node.getQuery(), indent);

            return null;
        }

        @Override
        public Void visitDelete(Delete node, Integer indent) {
            builder.append("DELETE FROM ")
                    .append(formatName(node.getTable().getName()));

            node.getWhere().ifPresent(where -> builder
                    .append(" WHERE ")
                    .append(ExpressionFormatter.formatExpression(where)));

            return null;
        }

        @Override
        public Void visitCreateTableAsSelect(CreateTableAsSelect node, Integer indent) {
            builder.append("CREATE TABLE ");
            if (node.isNotExists()) {
                builder.append("IF NOT EXISTS ");
            }
            builder.append(formatName(node.getName()));

            node.getColumnAliases().ifPresent(columnAliases -> {
                String columnList = columnAliases.stream()
                        .map(ExpressionFormatter::formatExpression)
                        .collect(joining(", "));
                builder.append(format("( %s )", columnList));
            });

            node.getComment().ifPresent(comment -> builder
                    .append("\nCOMMENT ")
                    .append(ExpressionFormatter.formatStringLiteral(comment)));
            builder.append(formatPropertiesMultiLine(node.getProperties()));

            builder.append(" AS ");
            process(node.getQuery(), indent);

            if (!node.isWithData()) {
                builder.append(" WITH NO DATA");
            }

            return null;
        }

        private String formatPropertiesMultiLine(List<Property> properties) {
            if (properties.isEmpty()) {
                return "";
            }

            String propertyList = properties.stream()
                    .map(element -> INDENT +
                            ExpressionFormatter.formatExpression(element.getName()) + " = " +
                            ExpressionFormatter.formatExpression(element.getValue()))
                    .collect(joining(",\n"));

            return "\nWITH (\n" + propertyList + "\n)";
        }

        private String formatPropertiesSingleLine(List<Property> properties) {
            if (properties.isEmpty()) {
                return "";
            }

            return " WITH ( " + joinProperties(properties) + " )";
        }

        private String joinProperties(List<Property> properties) {
            return properties.stream()
                    .map(element -> ExpressionFormatter.formatExpression(element.getName()) + " = " +
                            ExpressionFormatter.formatExpression(element.getValue()))
                    .collect(joining(", "));
        }

        @Override
        public Void visitInsert(Insert node, Integer indent) {
            builder.append("INSERT INTO ")
                    .append(formatName(node.getTarget()));

            node.getColumns().ifPresent(columns -> builder
                    .append(" (")
                    .append(Joiner.on(", ").join(columns))
                    .append(")"));

            builder.append("\n");

            process(node.getQuery(), indent);

            return null;
        }

        @Override
        public Void visitUpdate(Update node, Integer indent) {
            builder.append("UPDATE ")
                    .append(node.getTable().getName())
                    .append(" SET");
            int setCounter = node.getAssignments().size() - 1;
            for (UpdateAssignment assignment : node.getAssignments()) {
                builder.append("\n")
                        .append(indentString(indent + 1))
                        .append(assignment.getName().getValue())
                        .append(" = ")
                        .append(ExpressionFormatter.formatExpression(assignment.getValue()));
                if (setCounter > 0) {
                    builder.append(",");
                }
                setCounter--;
            }
            node.getWhere().ifPresent(where -> builder
                    .append("\n")
                    .append(indentString(indent))
                    .append("WHERE ").append(ExpressionFormatter.formatExpression(where)));
            return null;
        }

        @Override
        public Void visitRow(Row node, Integer indent) {
            builder.append("ROW(");
            boolean firstItem = true;
            for (Expression item : node.getItems()) {
                if (!firstItem) {
                    builder.append(", ");
                }
                process(item, indent);
                firstItem = false;
            }
            builder.append(")");
            return null;
        }

        private void processRelation(Relation relation, Integer indent) {
            // TODO: handle this properly
            if (relation instanceof Table) {
                builder.append("TABLE ")
                        .append(((Table) relation).getName())
                        .append('\n');
            } else {
                process(relation, indent);
            }
        }

        private StringBuilder append(int indent, String value) {
            return builder.append(indentString(indent))
                    .append(value);
        }

        private static String indentString(int indent) {
            return Strings.repeat(INDENT, indent);
        }

        private void formatDefinitionList(List<String> elements, int indent) {
            if (elements.size() == 1) {
                builder.append(" ")
                        .append(getOnlyElement(elements))
                        .append("\n");
            } else {
                builder.append("\n");
                for (int i = 0; i < elements.size() - 1; i++) {
                    append(indent, elements.get(i))
                            .append(",\n");
                }
                append(indent, elements.get(elements.size() - 1))
                        .append("\n");
            }
        }
    }

    private static void appendAliasColumns(StringBuilder builder, List<Identifier> columns) {
        if ((columns != null) && (!columns.isEmpty())) {
            String formattedColumns = columns.stream()
                    .map(ExpressionFormatter::formatExpression)
                    .collect(Collectors.joining(", "));

            builder.append(" (")
                    .append(formattedColumns)
                    .append(')');
        }
    }
}
