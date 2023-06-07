package com.github.melin.sqlflow.parser;

import com.github.melin.sqlflow.autogen.SqlFlowStatementBaseVisitor;
import com.github.melin.sqlflow.autogen.SqlFlowStatementLexer;
import com.github.melin.sqlflow.autogen.SqlFlowStatementParser;
import com.github.melin.sqlflow.tree.*;
import com.github.melin.sqlflow.tree.expression.*;
import com.github.melin.sqlflow.tree.filter.FetchFirst;
import com.github.melin.sqlflow.tree.filter.Limit;
import com.github.melin.sqlflow.tree.filter.Offset;
import com.github.melin.sqlflow.tree.group.*;
import com.github.melin.sqlflow.tree.join.Join;
import com.github.melin.sqlflow.tree.join.JoinCriteria;
import com.github.melin.sqlflow.tree.join.JoinOn;
import com.github.melin.sqlflow.tree.join.JoinUsing;
import com.github.melin.sqlflow.tree.literal.*;
import com.github.melin.sqlflow.tree.merge.MergeCase;
import com.github.melin.sqlflow.tree.merge.MergeDelete;
import com.github.melin.sqlflow.tree.merge.MergeInsert;
import com.github.melin.sqlflow.tree.merge.MergeUpdate;
import com.github.melin.sqlflow.tree.relation.*;
import com.github.melin.sqlflow.tree.statement.*;
import com.github.melin.sqlflow.tree.type.*;
import com.github.melin.sqlflow.tree.window.*;
import com.github.melin.sqlflow.tree.window.rowPattern.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.*;
import java.util.function.Function;

import static com.github.melin.sqlflow.autogen.SqlFlowStatementLexer.TIME;
import static com.github.melin.sqlflow.autogen.SqlFlowStatementLexer.TIMESTAMP;
import static com.github.melin.sqlflow.autogen.SqlFlowStatementLexer.SYSTEM_TIME;
import static com.github.melin.sqlflow.autogen.SqlFlowStatementLexer.VERSION;
import static com.github.melin.sqlflow.tree.window.SkipTo.*;
import static com.github.melin.sqlflow.tree.window.rowPattern.AnchorPattern.Type.PARTITION_END;
import static com.github.melin.sqlflow.tree.window.rowPattern.AnchorPattern.Type.PARTITION_START;
import static com.github.melin.sqlflow.tree.window.rowPattern.PatternSearchMode.Mode.INITIAL;
import static com.github.melin.sqlflow.tree.window.rowPattern.PatternSearchMode.Mode.SEEK;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * huaixin 2021/12/18 9:50 PM
 */

public class AstBuilder extends SqlFlowStatementBaseVisitor<Node> {

    private int parameterPosition;

    private final ParsingOptions parsingOptions;

    AstBuilder(ParsingOptions parsingOptions) {
        this.parsingOptions = requireNonNull(parsingOptions, "parsingOptions is null");
    }

    @Override
    public Node visitSingleStatement(SqlFlowStatementParser.SingleStatementContext context) {
        return visit(context.statement());
    }

    @Override
    public Node visitStandaloneExpression(SqlFlowStatementParser.StandaloneExpressionContext context) {
        return visit(context.expression());
    }

    @Override
    public Node visitStandaloneType(SqlFlowStatementParser.StandaloneTypeContext context) {
        return visit(context.type());
    }

    @Override
    public Node visitStandalonePathSpecification(SqlFlowStatementParser.StandalonePathSpecificationContext context) {
        return visit(context.pathSpecification());
    }

    @Override
    public Node visitStandaloneRowPattern(SqlFlowStatementParser.StandaloneRowPatternContext context) {
        return visit(context.rowPattern());
    }

    // ******************* statements **********************

    @Override
    public Node visitCreateTableAsSelect(SqlFlowStatementParser.CreateTableAsSelectContext context) {
        Optional<String> comment = Optional.empty();
        if (context.COMMENT() != null) {
            comment = Optional.of(((StringLiteral) visit(context.string())).getValue());
        }

        Optional<List<Identifier>> columnAliases = Optional.empty();
        if (context.columnAliases() != null) {
            columnAliases = Optional.of(visit(context.columnAliases().identifier(), Identifier.class));
        }

        List<Property> properties = ImmutableList.of();
        if (context.properties() != null) {
            properties = visit(context.properties().propertyAssignments().property(), Property.class);
        }

        return new CreateTableAsSelect(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                (Query) visit(context.query()),
                context.EXISTS() != null,
                properties,
                context.NO() == null,
                columnAliases,
                comment);
    }

    @Override
    public Node visitCreateMaterializedView(SqlFlowStatementParser.CreateMaterializedViewContext context) {
        Optional<String> comment = Optional.empty();
        if (context.COMMENT() != null) {
            comment = Optional.of(((StringLiteral) visit(context.string())).getValue());
        }

        List<Property> properties = ImmutableList.of();
        if (context.properties() != null) {
            properties = visit(context.properties().propertyAssignments().property(), Property.class);
        }

        return new CreateMaterializedView(
                Optional.of(getLocation(context)),
                getQualifiedName(context.qualifiedName()),
                (Query) visit(context.query()),
                context.REPLACE() != null,
                context.EXISTS() != null,
                properties,
                comment);
    }

    @Override
    public Node visitInsertInto(SqlFlowStatementParser.InsertIntoContext context) {
        Optional<List<Identifier>> columnAliases = Optional.empty();
        if (context.columnAliases() != null) {
            columnAliases = Optional.of(visit(context.columnAliases().identifier(), Identifier.class));
        }

        return new Insert(
                new Table(getQualifiedName(context.qualifiedName())),
                columnAliases,
                (Query) visit(context.query()));
    }

    @Override
    public Node visitDelete(SqlFlowStatementParser.DeleteContext context) {
        return new Delete(
                getLocation(context),
                new Table(getLocation(context), getQualifiedName(context.qualifiedName())),
                visitIfPresent(context.booleanExpression(), Expression.class));
    }

    @Override
    public Node visitUpdate(SqlFlowStatementParser.UpdateContext context) {
        return new Update(
                getLocation(context),
                new Table(getLocation(context), getQualifiedName(context.qualifiedName())),
                visit(context.updateAssignment(), UpdateAssignment.class),
                visitIfPresent(context.booleanExpression(), Expression.class));
    }

    @Override
    public Node visitUpdateAssignment(SqlFlowStatementParser.UpdateAssignmentContext context) {
        return new UpdateAssignment((Identifier) visit(context.identifier()), (Expression) visit(context.expression()));
    }

    @Override
    public Node visitMerge(SqlFlowStatementParser.MergeContext context) {
        return new Merge(
                getLocation(context),
                new Table(getLocation(context), getQualifiedName(context.qualifiedName())),
                visitIfPresent(context.identifier(), Identifier.class),
                (Relation) visit(context.relation()),
                (Expression) visit(context.expression()),
                visit(context.mergeCase(), MergeCase.class));
    }

    @Override
    public Node visitMergeInsert(SqlFlowStatementParser.MergeInsertContext context) {
        return new MergeInsert(
                getLocation(context),
                visitIfPresent(context.condition, Expression.class),
                visitIdentifiers(context.targets),
                visit(context.values, Expression.class));
    }

    private List<Identifier> visitIdentifiers(List<SqlFlowStatementParser.IdentifierContext> identifiers) {
        return identifiers.stream()
                .map(identifier -> (Identifier) visit(identifier))
                .collect(toImmutableList());
    }

    @Override
    public Node visitMergeUpdate(SqlFlowStatementParser.MergeUpdateContext context) {
        ImmutableList.Builder<MergeUpdate.Assignment> assignments = ImmutableList.builder();
        for (int i = 0; i < context.targets.size(); i++) {
            assignments.add(new MergeUpdate.Assignment(
                    (Identifier) visit(context.targets.get(i)),
                    (Expression) visit(context.values.get(i))));
        }

        return new MergeUpdate(getLocation(context), visitIfPresent(context.condition, Expression.class), assignments.build());
    }

    @Override
    public Node visitMergeDelete(SqlFlowStatementParser.MergeDeleteContext context) {
        return new MergeDelete(getLocation(context), visitIfPresent(context.condition, Expression.class));
    }

    @Override
    public Node visitCreateView(SqlFlowStatementParser.CreateViewContext context) {
        Optional<String> comment = Optional.empty();
        if (context.COMMENT() != null) {
            comment = Optional.of(((StringLiteral) visit(context.string())).getValue());
        }

        Optional<CreateView.Security> security = Optional.empty();
        if (context.DEFINER() != null) {
            security = Optional.of(CreateView.Security.DEFINER);
        } else if (context.INVOKER() != null) {
            security = Optional.of(CreateView.Security.INVOKER);
        }

        return new CreateView(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                (Query) visit(context.query()),
                context.REPLACE() != null,
                comment,
                security);
    }

    @Override
    public Node visitProperty(SqlFlowStatementParser.PropertyContext context) {
        return new Property(getLocation(context), (Identifier) visit(context.identifier()), (Expression) visit(context.expression()));
    }

    // ********************** query expressions ********************

    @Override
    public Node visitQuery(SqlFlowStatementParser.QueryContext context) {
        Query body = (Query) visit(context.queryNoWith());

        return new Query(
                getLocation(context),
                visitIfPresent(context.with(), With.class),
                body.getQueryBody(),
                body.getOrderBy(),
                body.getOffset(),
                body.getLimit());
    }

    @Override
    public Node visitWith(SqlFlowStatementParser.WithContext context) {
        return new With(getLocation(context), context.RECURSIVE() != null, visit(context.namedQuery(), WithQuery.class));
    }

    @Override
    public Node visitNamedQuery(SqlFlowStatementParser.NamedQueryContext context) {
        Optional<List<Identifier>> columns = Optional.empty();
        if (context.columnAliases() != null) {
            columns = Optional.of(visit(context.columnAliases().identifier(), Identifier.class));
        }

        return new WithQuery(
                getLocation(context),
                (Identifier) visit(context.name),
                (Query) visit(context.query()),
                columns);
    }

    @Override
    public Node visitQueryNoWith(SqlFlowStatementParser.QueryNoWithContext context) {
        QueryBody term = (QueryBody) visit(context.queryTerm());

        Optional<OrderBy> orderBy = Optional.empty();
        if (context.ORDER() != null) {
            orderBy = Optional.of(new OrderBy(getLocation(context.ORDER()), visit(context.sortItem(), SortItem.class)));
        }

        Optional<Offset> offset = Optional.empty();
        if (context.OFFSET() != null) {
            Expression rowCount;
            if (context.offset.INTEGER_VALUE() != null) {
                rowCount = new LongLiteral(getLocation(context.offset.INTEGER_VALUE()), context.offset.getText());
            } else {
                rowCount = new Parameter(getLocation(context.offset.QUESTION_MARK()), parameterPosition);
                parameterPosition++;
            }
            offset = Optional.of(new Offset(Optional.of(getLocation(context.OFFSET())), rowCount));
        }

        Optional<Node> limit = Optional.empty();
        if (context.FETCH() != null) {
            Optional<Expression> rowCount = Optional.empty();
            if (context.fetchFirst != null) {
                if (context.fetchFirst.INTEGER_VALUE() != null) {
                    rowCount = Optional.of(new LongLiteral(getLocation(context.fetchFirst.INTEGER_VALUE()), context.fetchFirst.getText()));
                } else {
                    rowCount = Optional.of(new Parameter(getLocation(context.fetchFirst.QUESTION_MARK()), parameterPosition));
                    parameterPosition++;
                }
            }
            limit = Optional.of(new FetchFirst(Optional.of(getLocation(context.FETCH())), rowCount, context.TIES() != null));
        } else if (context.LIMIT() != null) {
            if (context.limit == null) {
                throw new IllegalStateException("Missing LIMIT value");
            }
            Expression rowCount;
            if (context.limit.ALL() != null) {
                rowCount = new AllRows(getLocation(context.limit.ALL()));
            } else if (context.limit.rowCount().INTEGER_VALUE() != null) {
                rowCount = new LongLiteral(getLocation(context.limit.rowCount().INTEGER_VALUE()), context.limit.getText());
            } else {
                rowCount = new Parameter(getLocation(context.limit.rowCount().QUESTION_MARK()), parameterPosition);
                parameterPosition++;
            }

            limit = Optional.of(new Limit(Optional.of(getLocation(context.LIMIT())), rowCount));
        }

        if (term instanceof QuerySpecification) {
            // When we have a simple query specification
            // followed by order by, offset, limit or fetch,
            // fold the order by, limit, offset or fetch clauses
            // into the query specification (analyzer/planner
            // expects this structure to resolve references with respect
            // to columns defined in the query specification)
            QuerySpecification query = (QuerySpecification) term;

            return new Query(
                    getLocation(context),
                    Optional.empty(),
                    new QuerySpecification(
                            getLocation(context),
                            query.getSelect(),
                            query.getFrom(),
                            query.getWhere(),
                            query.getGroupBy(),
                            query.getHaving(),
                            query.getWindows(),
                            orderBy,
                            offset,
                            limit),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());
        }

        return new Query(
                getLocation(context),
                Optional.empty(),
                term,
                orderBy,
                offset,
                limit);
    }

    @Override
    public Node visitQuerySpecification(SqlFlowStatementParser.QuerySpecificationContext context) {
        Optional<Relation> from = Optional.empty();
        List<SelectItem> selectItems = visit(context.selectItem(), SelectItem.class);

        List<Relation> relations = visit(context.relation(), Relation.class);
        if (!relations.isEmpty()) {
            // synthesize implicit join nodes
            Iterator<Relation> iterator = relations.iterator();
            Relation relation = iterator.next();

            while (iterator.hasNext()) {
                relation = new Join(getLocation(context), Join.Type.IMPLICIT, relation, iterator.next(), Optional.empty());
            }

            from = Optional.of(relation);
        }

        return new QuerySpecification(
                getLocation(context),
                new Select(getLocation(context.SELECT()), isDistinct(context.setQuantifier()), selectItems),
                from,
                visitIfPresent(context.where, Expression.class),
                visitIfPresent(context.groupBy(), GroupBy.class),
                visitIfPresent(context.having, Expression.class),
                visit(context.windowDefinition(), WindowDefinition.class),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    @Override
    public Node visitGroupBy(SqlFlowStatementParser.GroupByContext context) {
        return new GroupBy(getLocation(context), isDistinct(context.setQuantifier()), visit(context.groupingElement(), GroupingElement.class));
    }

    @Override
    public Node visitSingleGroupingSet(SqlFlowStatementParser.SingleGroupingSetContext context) {
        return new SimpleGroupBy(getLocation(context), visit(context.groupingSet().expression(), Expression.class));
    }

    @Override
    public Node visitRollup(SqlFlowStatementParser.RollupContext context) {
        return new Rollup(getLocation(context), visit(context.expression(), Expression.class));
    }

    @Override
    public Node visitCube(SqlFlowStatementParser.CubeContext context) {
        return new Cube(getLocation(context), visit(context.expression(), Expression.class));
    }

    @Override
    public Node visitMultipleGroupingSets(SqlFlowStatementParser.MultipleGroupingSetsContext context) {
        return new GroupingSets(getLocation(context), context.groupingSet().stream()
                .map(groupingSet -> visit(groupingSet.expression(), Expression.class))
                .collect(toList()));
    }

    @Override
    public Node visitWindowSpecification(SqlFlowStatementParser.WindowSpecificationContext context) {
        Optional<OrderBy> orderBy = Optional.empty();
        if (context.ORDER() != null) {
            orderBy = Optional.of(new OrderBy(getLocation(context.ORDER()), visit(context.sortItem(), SortItem.class)));
        }

        return new WindowSpecification(
                getLocation(context),
                visitIfPresent(context.existingWindowName, Identifier.class),
                visit(context.partition, Expression.class),
                orderBy,
                visitIfPresent(context.windowFrame(), WindowFrame.class));
    }

    @Override
    public Node visitWindowDefinition(SqlFlowStatementParser.WindowDefinitionContext context) {
        return new WindowDefinition(
                getLocation(context),
                (Identifier) visit(context.name),
                (WindowSpecification) visit(context.windowSpecification()));
    }

    @Override
    public Node visitSetOperation(SqlFlowStatementParser.SetOperationContext context) {
        QueryBody left = (QueryBody) visit(context.left);
        QueryBody right = (QueryBody) visit(context.right);

        boolean distinct = context.setQuantifier() == null || context.setQuantifier().DISTINCT() != null;

        switch (context.operator.getType()) {
            case SqlFlowStatementLexer.UNION:
                return new Union(getLocation(context.UNION()), ImmutableList.of(left, right), distinct);
            case SqlFlowStatementLexer.INTERSECT:
                return new Intersect(getLocation(context.INTERSECT()), ImmutableList.of(left, right), distinct);
            case SqlFlowStatementLexer.EXCEPT:
                return new Except(getLocation(context.EXCEPT()), left, right, distinct);
        }

        throw new IllegalArgumentException("Unsupported set operation: " + context.operator.getText());
    }

    @Override
    public Node visitSelectAll(SqlFlowStatementParser.SelectAllContext context) {
        List<Identifier> aliases = ImmutableList.of();
        if (context.columnAliases() != null) {
            aliases = visit(context.columnAliases().identifier(), Identifier.class);
        }

        return new AllColumns(
                getLocation(context),
                visitIfPresent(context.primaryExpression(), Expression.class),
                aliases);
    }

    @Override
    public Node visitSelectSingle(SqlFlowStatementParser.SelectSingleContext context) {
        return new SingleColumn(
                getLocation(context),
                (Expression) visit(context.expression()),
                visitIfPresent(context.identifier(), Identifier.class));
    }

    @Override
    public Node visitTable(SqlFlowStatementParser.TableContext context) {
        return new Table(getLocation(context), getQualifiedName(context.qualifiedName()));
    }

    @Override
    public Node visitSubquery(SqlFlowStatementParser.SubqueryContext context) {
        return new TableSubquery(getLocation(context), (Query) visit(context.queryNoWith()));
    }

    @Override
    public Node visitInlineTable(SqlFlowStatementParser.InlineTableContext context) {
        return new Values(getLocation(context), visit(context.expression(), Expression.class));
    }

    // ***************** boolean expressions ******************

    @Override
    public Node visitLogicalNot(SqlFlowStatementParser.LogicalNotContext context) {
        return new NotExpression(getLocation(context), (Expression) visit(context.booleanExpression()));
    }

    @Override
    public Node visitOr(SqlFlowStatementParser.OrContext context) {
        List<ParserRuleContext> terms = flatten(context, element -> {
            if (element instanceof SqlFlowStatementParser.OrContext) {
                SqlFlowStatementParser.OrContext or = (SqlFlowStatementParser.OrContext) element;
                return Optional.of(or.booleanExpression());
            }

            return Optional.empty();
        });

        return new LogicalExpression(getLocation(context), LogicalExpression.Operator.OR, visit(terms, Expression.class));
    }

    @Override
    public Node visitAnd(SqlFlowStatementParser.AndContext context) {
        List<ParserRuleContext> terms = flatten(context, element -> {
            if (element instanceof SqlFlowStatementParser.AndContext) {
                SqlFlowStatementParser.AndContext and = (SqlFlowStatementParser.AndContext) element;
                return Optional.of(and.booleanExpression());
            }

            return Optional.empty();
        });

        return new LogicalExpression(getLocation(context), LogicalExpression.Operator.AND, visit(terms, Expression.class));
    }

    private static List<ParserRuleContext> flatten(ParserRuleContext root, Function<ParserRuleContext, Optional<List<? extends ParserRuleContext>>> extractChildren) {
        List<ParserRuleContext> result = new ArrayList<>();
        Deque<ParserRuleContext> pending = new ArrayDeque<>();
        pending.push(root);

        while (!pending.isEmpty()) {
            ParserRuleContext next = pending.pop();

            Optional<List<? extends ParserRuleContext>> children = extractChildren.apply(next);
            if (!children.isPresent()) {
                result.add(next);
            } else {
                for (int i = children.get().size() - 1; i >= 0; i--) {
                    pending.push(children.get().get(i));
                }
            }
        }

        return result;
    }

    // *************** from clause *****************

    @Override
    public Node visitJoinRelation(SqlFlowStatementParser.JoinRelationContext context) {
        Relation left = (Relation) visit(context.left);
        Relation right;

        if (context.CROSS() != null) {
            right = (Relation) visit(context.right);
            return new Join(getLocation(context), Join.Type.CROSS, left, right, Optional.empty());
        }

        JoinCriteria criteria;
        if (context.NATURAL() != null) {
            right = (Relation) visit(context.right);
            criteria = new NaturalJoin();
        } else {
            right = (Relation) visit(context.rightRelation);
            if (context.joinCriteria().ON() != null) {
                criteria = new JoinOn((Expression) visit(context.joinCriteria().booleanExpression()));
            } else if (context.joinCriteria().USING() != null) {
                criteria = new JoinUsing(visit(context.joinCriteria().identifier(), Identifier.class));
            } else {
                throw new IllegalArgumentException("Unsupported join criteria");
            }
        }

        Join.Type joinType;
        if (context.joinType().LEFT() != null) {
            joinType = Join.Type.LEFT;
        } else if (context.joinType().RIGHT() != null) {
            joinType = Join.Type.RIGHT;
        } else if (context.joinType().FULL() != null) {
            joinType = Join.Type.FULL;
        } else {
            joinType = Join.Type.INNER;
        }

        return new Join(getLocation(context), joinType, left, right, Optional.of(criteria));
    }

    @Override
    public Node visitSampledRelation(SqlFlowStatementParser.SampledRelationContext context) {
        Relation child = (Relation) visit(context.patternRecognition());

        if (context.TABLESAMPLE() == null) {
            return child;
        }

        return new SampledRelation(
                getLocation(context),
                child,
                getSamplingMethod((Token) context.sampleType().getChild(0).getPayload()),
                (Expression) visit(context.percentage));
    }

    @Override
    public Node visitPatternRecognition(SqlFlowStatementParser.PatternRecognitionContext context) {
        Relation child = (Relation) visit(context.aliasedRelation());

        if (context.MATCH_RECOGNIZE() == null) {
            return child;
        }

        Optional<OrderBy> orderBy = Optional.empty();
        if (context.ORDER() != null) {
            orderBy = Optional.of(new OrderBy(getLocation(context.ORDER()), visit(context.sortItem(), SortItem.class)));
        }

        Optional<PatternSearchMode> searchMode = Optional.empty();
        if (context.INITIAL() != null) {
            searchMode = Optional.of(new PatternSearchMode(getLocation(context.INITIAL()), INITIAL));
        } else if (context.SEEK() != null) {
            searchMode = Optional.of(new PatternSearchMode(getLocation(context.SEEK()), SEEK));
        }

        PatternRecognitionRelation relation = new PatternRecognitionRelation(
                getLocation(context),
                child,
                visit(context.partition, Expression.class),
                orderBy,
                visit(context.measureDefinition(), MeasureDefinition.class),
                getRowsPerMatch(context.rowsPerMatch()),
                visitIfPresent(context.skipTo(), SkipTo.class),
                searchMode,
                (RowPattern) visit(context.rowPattern()),
                visit(context.subsetDefinition(), SubsetDefinition.class),
                visit(context.variableDefinition(), VariableDefinition.class));

        if (context.identifier() == null) {
            return relation;
        }

        List<Identifier> aliases = null;
        if (context.columnAliases() != null) {
            aliases = visit(context.columnAliases().identifier(), Identifier.class);
        }

        return new AliasedRelation(getLocation(context), relation, (Identifier) visit(context.identifier()), aliases);
    }

    @Override
    public Node visitMeasureDefinition(SqlFlowStatementParser.MeasureDefinitionContext context) {
        return new MeasureDefinition(getLocation(context), (Expression) visit(context.expression()), (Identifier) visit(context.identifier()));
    }

    private Optional<PatternRecognitionRelation.RowsPerMatch> getRowsPerMatch(SqlFlowStatementParser.RowsPerMatchContext context) {
        if (context == null) {
            return Optional.empty();
        }

        if (context.ONE() != null) {
            return Optional.of(PatternRecognitionRelation.RowsPerMatch.ONE);
        }

        if (context.emptyMatchHandling() == null) {
            return Optional.of(PatternRecognitionRelation.RowsPerMatch.ALL_SHOW_EMPTY);
        }

        if (context.emptyMatchHandling().SHOW() != null) {
            return Optional.of(PatternRecognitionRelation.RowsPerMatch.ALL_SHOW_EMPTY);
        }

        if (context.emptyMatchHandling().OMIT() != null) {
            return Optional.of(PatternRecognitionRelation.RowsPerMatch.ALL_OMIT_EMPTY);
        }

        return Optional.of(PatternRecognitionRelation.RowsPerMatch.ALL_WITH_UNMATCHED);
    }

    @Override
    public Node visitSkipTo(SqlFlowStatementParser.SkipToContext context) {
        if (context.PAST() != null) {
            return skipPastLastRow(getLocation(context));
        }

        if (context.NEXT() != null) {
            return skipToNextRow(getLocation(context));
        }

        if (context.FIRST() != null) {
            return skipToFirst(getLocation(context), (Identifier) visit(context.identifier()));
        }

        return skipToLast(getLocation(context), (Identifier) visit(context.identifier()));
    }

    @Override
    public Node visitSubsetDefinition(SqlFlowStatementParser.SubsetDefinitionContext context) {
        return new SubsetDefinition(getLocation(context), (Identifier) visit(context.name), visit(context.union, Identifier.class));
    }

    @Override
    public Node visitVariableDefinition(SqlFlowStatementParser.VariableDefinitionContext context) {
        return new VariableDefinition(getLocation(context), (Identifier) visit(context.identifier()), (Expression) visit(context.expression()));
    }

    @Override
    public Node visitAliasedRelation(SqlFlowStatementParser.AliasedRelationContext context) {
        Relation child = (Relation) visit(context.relationPrimary());

        if (context.identifier() == null) {
            return child;
        }

        List<Identifier> aliases = null;
        if (context.columnAliases() != null) {
            aliases = visit(context.columnAliases().identifier(), Identifier.class);
        }

        return new AliasedRelation(getLocation(context), child, (Identifier) visit(context.identifier()), aliases);
    }

    @Override
    public Node visitTableName(SqlFlowStatementParser.TableNameContext context) {
        if (context.queryPeriod() != null) {
            return new Table(getLocation(context), getQualifiedName(context.qualifiedName()), (QueryPeriod) visit(context.queryPeriod()));
        }
        return new Table(getLocation(context), getQualifiedName(context.qualifiedName()));
    }

    @Override
    public Node visitSubqueryRelation(SqlFlowStatementParser.SubqueryRelationContext context) {
        return new TableSubquery(getLocation(context), (Query) visit(context.query()));
    }

    @Override
    public Node visitUnnest(SqlFlowStatementParser.UnnestContext context) {
        return new Unnest(getLocation(context), visit(context.expression(), Expression.class), context.ORDINALITY() != null);
    }

    @Override
    public Node visitLateral(SqlFlowStatementParser.LateralContext context) {
        return new Lateral(getLocation(context), (Query) visit(context.query()));
    }

    @Override
    public Node visitParenthesizedRelation(SqlFlowStatementParser.ParenthesizedRelationContext context) {
        return visit(context.relation());
    }

    // ********************* predicates *******************

    @Override
    public Node visitPredicated(SqlFlowStatementParser.PredicatedContext context) {
        if (context.predicate() != null) {
            return visit(context.predicate());
        }

        return visit(context.valueExpression);
    }

    @Override
    public Node visitComparison(SqlFlowStatementParser.ComparisonContext context) {
        return new ComparisonExpression(
                getLocation(context.comparisonOperator()),
                getComparisonOperator(((TerminalNode) context.comparisonOperator().getChild(0)).getSymbol()),
                (Expression) visit(context.value),
                (Expression) visit(context.right));
    }

    @Override
    public Node visitDistinctFrom(SqlFlowStatementParser.DistinctFromContext context) {
        Expression expression = new ComparisonExpression(
                getLocation(context),
                ComparisonExpression.Operator.IS_DISTINCT_FROM,
                (Expression) visit(context.value),
                (Expression) visit(context.right));

        if (context.NOT() != null) {
            expression = new NotExpression(getLocation(context), expression);
        }

        return expression;
    }

    @Override
    public Node visitBetween(SqlFlowStatementParser.BetweenContext context) {
        Expression expression = new BetweenPredicate(
                getLocation(context),
                (Expression) visit(context.value),
                (Expression) visit(context.lower),
                (Expression) visit(context.upper));

        if (context.NOT() != null) {
            expression = new NotExpression(getLocation(context), expression);
        }

        return expression;
    }

    @Override
    public Node visitNullPredicate(SqlFlowStatementParser.NullPredicateContext context) {
        Expression child = (Expression) visit(context.value);

        if (context.NOT() == null) {
            return new IsNullPredicate(getLocation(context), child);
        }

        return new IsNotNullPredicate(getLocation(context), child);
    }

    @Override
    public Node visitLike(SqlFlowStatementParser.LikeContext context) {
        Expression result = new LikePredicate(
                getLocation(context),
                (Expression) visit(context.value),
                (Expression) visit(context.pattern),
                visitIfPresent(context.escape, Expression.class));

        if (context.NOT() != null) {
            result = new NotExpression(getLocation(context), result);
        }

        return result;
    }

    @Override
    public Node visitInList(SqlFlowStatementParser.InListContext context) {
        Expression result = new InPredicate(
                getLocation(context),
                (Expression) visit(context.value),
                new InListExpression(getLocation(context), visit(context.expression(), Expression.class)));

        if (context.NOT() != null) {
            result = new NotExpression(getLocation(context), result);
        }

        return result;
    }

    @Override
    public Node visitInSubquery(SqlFlowStatementParser.InSubqueryContext context) {
        Expression result = new InPredicate(
                getLocation(context),
                (Expression) visit(context.value),
                new SubqueryExpression(getLocation(context), (Query) visit(context.query())));

        if (context.NOT() != null) {
            result = new NotExpression(getLocation(context), result);
        }

        return result;
    }

    @Override
    public Node visitExists(SqlFlowStatementParser.ExistsContext context) {
        return new ExistsPredicate(getLocation(context), new SubqueryExpression(getLocation(context), (Query) visit(context.query())));
    }

    @Override
    public Node visitQuantifiedComparison(SqlFlowStatementParser.QuantifiedComparisonContext context) {
        return new QuantifiedComparisonExpression(
                getLocation(context.comparisonOperator()),
                getComparisonOperator(((TerminalNode) context.comparisonOperator().getChild(0)).getSymbol()),
                getComparisonQuantifier(((TerminalNode) context.comparisonQuantifier().getChild(0)).getSymbol()),
                (Expression) visit(context.value),
                new SubqueryExpression(getLocation(context.query()), (Query) visit(context.query())));
    }

    // ************** value expressions **************

    @Override
    public Node visitArithmeticUnary(SqlFlowStatementParser.ArithmeticUnaryContext context) {
        Expression child = (Expression) visit(context.valueExpression());

        switch (context.operator.getType()) {
            case SqlFlowStatementLexer.MINUS:
                return ArithmeticUnaryExpression.negative(getLocation(context), child);
            case SqlFlowStatementLexer.PLUS:
                return ArithmeticUnaryExpression.positive(getLocation(context), child);
            default:
                throw new UnsupportedOperationException("Unsupported sign: " + context.operator.getText());
        }
    }

    @Override
    public Node visitArithmeticBinary(SqlFlowStatementParser.ArithmeticBinaryContext context) {
        return new ArithmeticBinaryExpression(
                getLocation(context.operator),
                getArithmeticBinaryOperator(context.operator),
                (Expression) visit(context.left),
                (Expression) visit(context.right));
    }

    @Override
    public Node visitConcatenation(SqlFlowStatementParser.ConcatenationContext context) {
        return new FunctionCall(
                getLocation(context.CONCAT()),
                QualifiedName.of("concat"), ImmutableList.of(
                (Expression) visit(context.left),
                (Expression) visit(context.right)));
    }

    @Override
    public Node visitAtTimeZone(SqlFlowStatementParser.AtTimeZoneContext context) {
        return new AtTimeZone(
                getLocation(context.AT()),
                (Expression) visit(context.valueExpression()),
                (Expression) visit(context.timeZoneSpecifier()));
    }

    @Override
    public Node visitTimeZoneInterval(SqlFlowStatementParser.TimeZoneIntervalContext context) {
        return visit(context.interval());
    }

    @Override
    public Node visitTimeZoneString(SqlFlowStatementParser.TimeZoneStringContext context) {
        return visit(context.string());
    }

    // ********************* primary expressions **********************

    @Override
    public Node visitParenthesizedExpression(SqlFlowStatementParser.ParenthesizedExpressionContext context) {
        return visit(context.expression());
    }

    @Override
    public Node visitRowConstructor(SqlFlowStatementParser.RowConstructorContext context) {
        return new Row(getLocation(context), visit(context.expression(), Expression.class));
    }

    @Override
    public Node visitArrayConstructor(SqlFlowStatementParser.ArrayConstructorContext context) {
        return new ArrayConstructor(getLocation(context), visit(context.expression(), Expression.class));
    }

    @Override
    public Node visitCast(SqlFlowStatementParser.CastContext context) {
        boolean isTryCast = context.TRY_CAST() != null;
        return new Cast(getLocation(context), (Expression) visit(context.expression()), (DataType) visit(context.type()), isTryCast);
    }

    @Override
    public Node visitSpecialDateTimeFunction(SqlFlowStatementParser.SpecialDateTimeFunctionContext context) {
        CurrentTime.Function function = getDateTimeFunctionType(context.name);

        if (context.precision != null) {
            return new CurrentTime(getLocation(context), function, Integer.parseInt(context.precision.getText()));
        }

        return new CurrentTime(getLocation(context), function);
    }

    @Override
    public Node visitCurrentCatalog(SqlFlowStatementParser.CurrentCatalogContext context) {
        return new CurrentCatalog(getLocation(context.CURRENT_CATALOG()));
    }

    @Override
    public Node visitCurrentSchema(SqlFlowStatementParser.CurrentSchemaContext context) {
        return new CurrentSchema(getLocation(context.CURRENT_SCHEMA()));
    }

    @Override
    public Node visitCurrentUser(SqlFlowStatementParser.CurrentUserContext context) {
        return new CurrentUser(getLocation(context.CURRENT_USER()));
    }

    @Override
    public Node visitCurrentPath(SqlFlowStatementParser.CurrentPathContext context) {
        return new CurrentPath(getLocation(context.CURRENT_PATH()));
    }

    @Override
    public Node visitExtract(SqlFlowStatementParser.ExtractContext context) {
        String fieldString = context.identifier().getText();
        Extract.Field field;
        try {
            field = Extract.Field.valueOf(fieldString.toUpperCase(ENGLISH));
        } catch (IllegalArgumentException e) {
            throw parseError("Invalid EXTRACT field: " + fieldString, context);
        }
        return new Extract(getLocation(context), (Expression) visit(context.valueExpression()), field);
    }

    /**
     * Returns the corresponding {@link FunctionCall} for the `LISTAGG` primary expression.
     * <p>
     * Although the syntax tree should represent the structure of the original parsed query
     * as closely as possible and any semantic interpretation should be part of the
     * analysis/planning phase, in case of `LISTAGG` aggregation function it is more pragmatic
     * now to create a synthetic {@link FunctionCall} expression during the parsing of the syntax tree.
     *
     * @param context `LISTAGG` expression context
     */
    @Override
    public Node visitListagg(SqlFlowStatementParser.ListaggContext context) {
        Optional<Window> window = Optional.empty();
        OrderBy orderBy = new OrderBy(visit(context.sortItem(), SortItem.class));
        boolean distinct = isDistinct(context.setQuantifier());

        Expression expression = (Expression) visit(context.expression());
        StringLiteral separator = context.string() == null ? new StringLiteral(getLocation(context), "") : (StringLiteral) (visit(context.string()));
        BooleanLiteral overflowError = new BooleanLiteral(getLocation(context), "true");
        StringLiteral overflowFiller = new StringLiteral(getLocation(context), "...");
        BooleanLiteral showOverflowEntryCount = new BooleanLiteral(getLocation(context), "false");

        SqlFlowStatementParser.ListAggOverflowBehaviorContext overflowBehavior = context.listAggOverflowBehavior();
        if (overflowBehavior != null) {
            if (overflowBehavior.ERROR() != null) {
                overflowError = new BooleanLiteral(getLocation(context), "true");
            } else if (overflowBehavior.TRUNCATE() != null) {
                overflowError = new BooleanLiteral(getLocation(context), "false");
                if (overflowBehavior.string() != null) {
                    overflowFiller = (StringLiteral) (visit(overflowBehavior.string()));
                }
                SqlFlowStatementParser.ListaggCountIndicationContext listaggCountIndicationContext = overflowBehavior.listaggCountIndication();
                if (listaggCountIndicationContext.WITH() != null) {
                    showOverflowEntryCount = new BooleanLiteral(getLocation(context), "true");
                } else if (listaggCountIndicationContext.WITHOUT() != null) {
                    showOverflowEntryCount = new BooleanLiteral(getLocation(context), "false");
                }
            }
        }

        List<Expression> arguments = ImmutableList.of(expression, separator, overflowError, overflowFiller, showOverflowEntryCount);

        //TODO model this as a ListAgg node in the AST
        return new FunctionCall(
                Optional.of(getLocation(context)),
                QualifiedName.of("LISTAGG"),
                window,
                Optional.empty(),
                Optional.of(orderBy),
                distinct,
                Optional.empty(),
                Optional.empty(),
                arguments);
    }

    @Override
    public Node visitSubstring(SqlFlowStatementParser.SubstringContext context) {
        return new FunctionCall(getLocation(context), QualifiedName.of("substr"), visit(context.valueExpression(), Expression.class));
    }

    @Override
    public Node visitPosition(SqlFlowStatementParser.PositionContext context) {
        List<Expression> arguments = Lists.reverse(visit(context.valueExpression(), Expression.class));
        return new FunctionCall(getLocation(context), QualifiedName.of("strpos"), arguments);
    }

    @Override
    public Node visitNormalize(SqlFlowStatementParser.NormalizeContext context) {
        Expression str = (Expression) visit(context.valueExpression());
        String normalForm = Optional.ofNullable(context.normalForm()).map(ParserRuleContext::getText).orElse("NFC");
        return new FunctionCall(
                getLocation(context),
                QualifiedName.of(ImmutableList.of(new Identifier("normalize", true))), // delimited to avoid ambiguity with NORMALIZE SQL construct
                ImmutableList.of(str, new StringLiteral(getLocation(context), normalForm)));
    }

    @Override
    public Node visitSubscript(SqlFlowStatementParser.SubscriptContext context) {
        return new SubscriptExpression(getLocation(context), (Expression) visit(context.value), (Expression) visit(context.index));
    }

    @Override
    public Node visitSubqueryExpression(SqlFlowStatementParser.SubqueryExpressionContext context) {
        return new SubqueryExpression(getLocation(context), (Query) visit(context.query()));
    }

    @Override
    public Node visitDereference(SqlFlowStatementParser.DereferenceContext context) {
        return new DereferenceExpression(
                getLocation(context),
                (Expression) visit(context.base),
                (Identifier) visit(context.fieldName));
    }

    @Override
    public Node visitColumnReference(SqlFlowStatementParser.ColumnReferenceContext context) {
        return visit(context.identifier());
    }

    @Override
    public Node visitSimpleCase(SqlFlowStatementParser.SimpleCaseContext context) {
        return new SimpleCaseExpression(
                getLocation(context),
                (Expression) visit(context.operand),
                visit(context.whenClause(), WhenClause.class),
                visitIfPresent(context.elseExpression, Expression.class));
    }

    @Override
    public Node visitSearchedCase(SqlFlowStatementParser.SearchedCaseContext context) {
        return new SearchedCaseExpression(
                getLocation(context),
                visit(context.whenClause(), WhenClause.class),
                visitIfPresent(context.elseExpression, Expression.class));
    }

    @Override
    public Node visitWhenClause(SqlFlowStatementParser.WhenClauseContext context) {
        return new WhenClause(getLocation(context), (Expression) visit(context.condition), (Expression) visit(context.result));
    }

    @Override
    public Node visitFunctionCall(SqlFlowStatementParser.FunctionCallContext context) {
        Optional<Expression> filter = visitIfPresent(context.filter(), Expression.class);
        Optional<Window> window = visitIfPresent(context.over(), Window.class);

        Optional<OrderBy> orderBy = Optional.empty();
        if (context.ORDER() != null) {
            orderBy = Optional.of(new OrderBy(visit(context.sortItem(), SortItem.class)));
        }

        QualifiedName name = getQualifiedName(context.qualifiedName());

        boolean distinct = isDistinct(context.setQuantifier());

        SqlFlowStatementParser.NullTreatmentContext nullTreatment = context.nullTreatment();

        SqlFlowStatementParser.ProcessingModeContext processingMode = context.processingMode();

        if (name.toString().equalsIgnoreCase("if")) {
            check(context.expression().size() == 2 || context.expression().size() == 3, "Invalid number of arguments for 'if' function", context);
            check(!window.isPresent(), "OVER clause not valid for 'if' function", context);
            check(!distinct, "DISTINCT not valid for 'if' function", context);
            check(nullTreatment == null, "Null treatment clause not valid for 'if' function", context);
            check(processingMode == null, "Running or final semantics not valid for 'if' function", context);
            check(!filter.isPresent(), "FILTER not valid for 'if' function", context);

            Expression elseExpression = null;
            if (context.expression().size() == 3) {
                elseExpression = (Expression) visit(context.expression(2));
            }

            return new IfExpression(
                    getLocation(context),
                    (Expression) visit(context.expression(0)),
                    (Expression) visit(context.expression(1)),
                    elseExpression);
        }

        if (name.toString().equalsIgnoreCase("nullif")) {
            check(context.expression().size() == 2, "Invalid number of arguments for 'nullif' function", context);
            check(!window.isPresent(), "OVER clause not valid for 'nullif' function", context);
            check(!distinct, "DISTINCT not valid for 'nullif' function", context);
            check(nullTreatment == null, "Null treatment clause not valid for 'nullif' function", context);
            check(processingMode == null, "Running or final semantics not valid for 'nullif' function", context);
            check(!filter.isPresent(), "FILTER not valid for 'nullif' function", context);

            return new NullIfExpression(
                    getLocation(context),
                    (Expression) visit(context.expression(0)),
                    (Expression) visit(context.expression(1)));
        }

        if (name.toString().equalsIgnoreCase("coalesce")) {
            check(context.expression().size() >= 2, "The 'coalesce' function must have at least two arguments", context);
            check(!window.isPresent(), "OVER clause not valid for 'coalesce' function", context);
            check(!distinct, "DISTINCT not valid for 'coalesce' function", context);
            check(nullTreatment == null, "Null treatment clause not valid for 'coalesce' function", context);
            check(processingMode == null, "Running or final semantics not valid for 'coalesce' function", context);
            check(!filter.isPresent(), "FILTER not valid for 'coalesce' function", context);

            return new CoalesceExpression(getLocation(context), visit(context.expression(), Expression.class));
        }

        if (name.toString().equalsIgnoreCase("try")) {
            check(context.expression().size() == 1, "The 'try' function must have exactly one argument", context);
            check(!window.isPresent(), "OVER clause not valid for 'try' function", context);
            check(!distinct, "DISTINCT not valid for 'try' function", context);
            check(nullTreatment == null, "Null treatment clause not valid for 'try' function", context);
            check(processingMode == null, "Running or final semantics not valid for 'try' function", context);
            check(!filter.isPresent(), "FILTER not valid for 'try' function", context);

            return new TryExpression(getLocation(context), (Expression) visit(getOnlyElement(context.expression())));
        }

        if (name.toString().equalsIgnoreCase("format")) {
            check(context.expression().size() >= 2, "The 'format' function must have at least two arguments", context);
            check(!window.isPresent(), "OVER clause not valid for 'format' function", context);
            check(!distinct, "DISTINCT not valid for 'format' function", context);
            check(nullTreatment == null, "Null treatment clause not valid for 'format' function", context);
            check(processingMode == null, "Running or final semantics not valid for 'format' function", context);
            check(!filter.isPresent(), "FILTER not valid for 'format' function", context);

            return new Format(getLocation(context), visit(context.expression(), Expression.class));
        }

        if (name.toString().equalsIgnoreCase("$internal$bind")) {
            check(context.expression().size() >= 1, "The '$internal$bind' function must have at least one arguments", context);
            check(!window.isPresent(), "OVER clause not valid for '$internal$bind' function", context);
            check(!distinct, "DISTINCT not valid for '$internal$bind' function", context);
            check(nullTreatment == null, "Null treatment clause not valid for '$internal$bind' function", context);
            check(processingMode == null, "Running or final semantics not valid for '$internal$bind' function", context);
            check(!filter.isPresent(), "FILTER not valid for '$internal$bind' function", context);

            int numValues = context.expression().size() - 1;
            List<Expression> arguments = context.expression().stream()
                    .map(this::visit)
                    .map(Expression.class::cast)
                    .collect(toImmutableList());

            return new BindExpression(
                    getLocation(context),
                    arguments.subList(0, numValues),
                    arguments.get(numValues));
        }

        Optional<FunctionCall.NullTreatment> nulls = Optional.empty();
        if (nullTreatment != null) {
            if (nullTreatment.IGNORE() != null) {
                nulls = Optional.of(FunctionCall.NullTreatment.IGNORE);
            } else if (nullTreatment.RESPECT() != null) {
                nulls = Optional.of(FunctionCall.NullTreatment.RESPECT);
            }
        }

        Optional<ProcessingMode> mode = Optional.empty();
        if (processingMode != null) {
            if (processingMode.RUNNING() != null) {
                mode = Optional.of(new ProcessingMode(getLocation(processingMode), ProcessingMode.Mode.RUNNING));
            } else if (processingMode.FINAL() != null) {
                mode = Optional.of(new ProcessingMode(getLocation(processingMode), ProcessingMode.Mode.FINAL));
            }
        }

        List<Expression> arguments = visit(context.expression(), Expression.class);
        if (context.label != null) {
            arguments = ImmutableList.of(new DereferenceExpression(getLocation(context.label), (Identifier) visit(context.label)));
        }

        return new FunctionCall(
                Optional.of(getLocation(context)),
                name,
                window,
                filter,
                orderBy,
                distinct,
                nulls,
                mode,
                arguments);
    }

    @Override
    public Node visitMeasure(SqlFlowStatementParser.MeasureContext context) {
        return new WindowOperation(getLocation(context), (Identifier) visit(context.identifier()), (Window) visit(context.over()));
    }

    @Override
    public Node visitLambda(SqlFlowStatementParser.LambdaContext context) {
        List<LambdaArgumentDeclaration> arguments = visit(context.identifier(), Identifier.class).stream()
                .map(LambdaArgumentDeclaration::new)
                .collect(toList());

        Expression body = (Expression) visit(context.expression());

        return new LambdaExpression(getLocation(context), arguments, body);
    }

    @Override
    public Node visitFilter(SqlFlowStatementParser.FilterContext context) {
        return visit(context.booleanExpression());
    }

    @Override
    public Node visitOver(SqlFlowStatementParser.OverContext context) {
        if (context.windowName != null) {
            return new WindowReference(getLocation(context), (Identifier) visit(context.windowName));
        }

        return visit(context.windowSpecification());
    }

    @Override
    public Node visitSortItem(SqlFlowStatementParser.SortItemContext context) {
        return new SortItem(
                getLocation(context),
                (Expression) visit(context.expression()),
                Optional.ofNullable(context.ordering)
                        .map(AstBuilder::getOrderingType)
                        .orElse(SortItem.Ordering.ASCENDING),
                Optional.ofNullable(context.nullOrdering)
                        .map(AstBuilder::getNullOrderingType)
                        .orElse(SortItem.NullOrdering.UNDEFINED));
    }

    @Override
    public Node visitWindowFrame(SqlFlowStatementParser.WindowFrameContext context) {
        Optional<PatternSearchMode> searchMode = Optional.empty();
        if (context.INITIAL() != null) {
            searchMode = Optional.of(new PatternSearchMode(getLocation(context.INITIAL()), INITIAL));
        } else if (context.SEEK() != null) {
            searchMode = Optional.of(new PatternSearchMode(getLocation(context.SEEK()), SEEK));
        }

        return new WindowFrame(
                getLocation(context),
                getFrameType(context.frameExtent().frameType),
                (FrameBound) visit(context.frameExtent().start),
                visitIfPresent(context.frameExtent().end, FrameBound.class),
                visit(context.measureDefinition(), MeasureDefinition.class),
                visitIfPresent(context.skipTo(), SkipTo.class),
                searchMode,
                visitIfPresent(context.rowPattern(), RowPattern.class),
                visit(context.subsetDefinition(), SubsetDefinition.class),
                visit(context.variableDefinition(), VariableDefinition.class));
    }

    @Override
    public Node visitUnboundedFrame(SqlFlowStatementParser.UnboundedFrameContext context) {
        return new FrameBound(getLocation(context), getUnboundedFrameBoundType(context.boundType));
    }

    @Override
    public Node visitBoundedFrame(SqlFlowStatementParser.BoundedFrameContext context) {
        return new FrameBound(getLocation(context), getBoundedFrameBoundType(context.boundType), (Expression) visit(context.expression()));
    }

    @Override
    public Node visitCurrentRowBound(SqlFlowStatementParser.CurrentRowBoundContext context) {
        return new FrameBound(getLocation(context), FrameBound.Type.CURRENT_ROW);
    }

    @Override
    public Node visitGroupingOperation(SqlFlowStatementParser.GroupingOperationContext context) {
        List<QualifiedName> arguments = context.qualifiedName().stream()
                .map(this::getQualifiedName)
                .collect(toList());

        return new GroupingOperation(Optional.of(getLocation(context)), arguments);
    }

    @Override
    public Node visitUnquotedIdentifier(SqlFlowStatementParser.UnquotedIdentifierContext context) {
        return new Identifier(getLocation(context), context.getText(), false);
    }

    @Override
    public Node visitQuotedIdentifier(SqlFlowStatementParser.QuotedIdentifierContext context) {
        String token = context.getText();
        String identifier = token.substring(1, token.length() - 1)
                .replace("\"\"", "\"");

        return new Identifier(getLocation(context), identifier, true);
    }

    @Override
    public Node visitPatternAlternation(SqlFlowStatementParser.PatternAlternationContext context) {
        List<RowPattern> parts = visit(context.rowPattern(), RowPattern.class);
        return new PatternAlternation(getLocation(context), parts);
    }

    @Override
    public Node visitPatternConcatenation(SqlFlowStatementParser.PatternConcatenationContext context) {
        List<RowPattern> parts = visit(context.rowPattern(), RowPattern.class);
        return new PatternConcatenation(getLocation(context), parts);
    }

    @Override
    public Node visitQuantifiedPrimary(SqlFlowStatementParser.QuantifiedPrimaryContext context) {
        RowPattern primary = (RowPattern) visit(context.patternPrimary());
        if (context.patternQuantifier() != null) {
            return new QuantifiedPattern(getLocation(context), primary, (PatternQuantifier) visit(context.patternQuantifier()));
        }
        return primary;
    }

    @Override
    public Node visitPatternVariable(SqlFlowStatementParser.PatternVariableContext context) {
        return new PatternVariable(getLocation(context), (Identifier) visit(context.identifier()));
    }

    @Override
    public Node visitEmptyPattern(SqlFlowStatementParser.EmptyPatternContext context) {
        return new EmptyPattern(getLocation(context));
    }

    @Override
    public Node visitPatternPermutation(SqlFlowStatementParser.PatternPermutationContext context) {
        return new PatternPermutation(getLocation(context), visit(context.rowPattern(), RowPattern.class));
    }

    @Override
    public Node visitGroupedPattern(SqlFlowStatementParser.GroupedPatternContext context) {
        // skip parentheses
        return visit(context.rowPattern());
    }

    @Override
    public Node visitPartitionStartAnchor(SqlFlowStatementParser.PartitionStartAnchorContext context) {
        return new AnchorPattern(getLocation(context), PARTITION_START);
    }

    @Override
    public Node visitPartitionEndAnchor(SqlFlowStatementParser.PartitionEndAnchorContext context) {
        return new AnchorPattern(getLocation(context), PARTITION_END);
    }

    @Override
    public Node visitExcludedPattern(SqlFlowStatementParser.ExcludedPatternContext context) {
        return new ExcludedPattern(getLocation(context), (RowPattern) visit(context.rowPattern()));
    }

    @Override
    public Node visitZeroOrMoreQuantifier(SqlFlowStatementParser.ZeroOrMoreQuantifierContext context) {
        boolean greedy = context.reluctant == null;
        return new ZeroOrMoreQuantifier(getLocation(context), greedy);
    }

    @Override
    public Node visitOneOrMoreQuantifier(SqlFlowStatementParser.OneOrMoreQuantifierContext context) {
        boolean greedy = context.reluctant == null;
        return new OneOrMoreQuantifier(getLocation(context), greedy);
    }

    @Override
    public Node visitZeroOrOneQuantifier(SqlFlowStatementParser.ZeroOrOneQuantifierContext context) {
        boolean greedy = context.reluctant == null;
        return new ZeroOrOneQuantifier(getLocation(context), greedy);
    }

    @Override
    public Node visitRangeQuantifier(SqlFlowStatementParser.RangeQuantifierContext context) {
        boolean greedy = context.reluctant == null;

        Optional<LongLiteral> atLeast = Optional.empty();
        Optional<LongLiteral> atMost = Optional.empty();
        if (context.exactly != null) {
            atLeast = Optional.of(new LongLiteral(getLocation(context.exactly), context.exactly.getText()));
            atMost = Optional.of(new LongLiteral(getLocation(context.exactly), context.exactly.getText()));
        }
        if (context.atLeast != null) {
            atLeast = Optional.of(new LongLiteral(getLocation(context.atLeast), context.atLeast.getText()));
        }
        if (context.atMost != null) {
            atMost = Optional.of(new LongLiteral(getLocation(context.atMost), context.atMost.getText()));
        }
        return new RangeQuantifier(getLocation(context), greedy, atLeast, atMost);
    }

    // ************** literals **************

    @Override
    public Node visitNullLiteral(SqlFlowStatementParser.NullLiteralContext context) {
        return new NullLiteral(getLocation(context));
    }

    @Override
    public Node visitBasicStringLiteral(SqlFlowStatementParser.BasicStringLiteralContext context) {
        return new StringLiteral(getLocation(context), unquote(context.STRING().getText()));
    }

    @Override
    public Node visitUnicodeStringLiteral(SqlFlowStatementParser.UnicodeStringLiteralContext context) {
        return new StringLiteral(getLocation(context), decodeUnicodeLiteral(context));
    }

    @Override
    public Node visitBinaryLiteral(SqlFlowStatementParser.BinaryLiteralContext context) {
        String raw = context.BINARY_LITERAL().getText();
        return new BinaryLiteral(getLocation(context), unquote(raw.substring(1)));
    }

    @Override
    public Node visitTypeConstructor(SqlFlowStatementParser.TypeConstructorContext context) {
        String value = ((StringLiteral) visit(context.string())).getValue();

        if (context.DOUBLE() != null) {
            // TODO: Temporary hack that should be removed with new planner.
            return new GenericLiteral(getLocation(context), "DOUBLE", value);
        }

        String type = context.identifier().getText();
        if (type.equalsIgnoreCase("time")) {
            return new TimeLiteral(getLocation(context), value);
        }
        if (type.equalsIgnoreCase("timestamp")) {
            return new TimestampLiteral(getLocation(context), value);
        }
        if (type.equalsIgnoreCase("decimal")) {
            return new DecimalLiteral(getLocation(context), value);
        }
        if (type.equalsIgnoreCase("char")) {
            return new CharLiteral(getLocation(context), value);
        }

        return new GenericLiteral(getLocation(context), type, value);
    }

    @Override
    public Node visitIntegerLiteral(SqlFlowStatementParser.IntegerLiteralContext context) {
        return new LongLiteral(getLocation(context), context.getText());
    }

    @Override
    public Node visitDecimalLiteral(SqlFlowStatementParser.DecimalLiteralContext context) {
        switch (parsingOptions.getDecimalLiteralTreatment()) {
            case AS_DOUBLE:
                return new DoubleLiteral(getLocation(context), context.getText());
            case AS_DECIMAL:
                return new DecimalLiteral(getLocation(context), context.getText());
            case REJECT:
                throw new ParsingException("Unexpected decimal literal: " + context.getText());
        }
        throw new AssertionError("Unreachable");
    }

    @Override
    public Node visitDoubleLiteral(SqlFlowStatementParser.DoubleLiteralContext context) {
        return new DoubleLiteral(getLocation(context), context.getText());
    }

    @Override
    public Node visitBooleanValue(SqlFlowStatementParser.BooleanValueContext context) {
        return new BooleanLiteral(getLocation(context), context.getText());
    }

    @Override
    public Node visitInterval(SqlFlowStatementParser.IntervalContext context) {
        return new IntervalLiteral(
                getLocation(context),
                ((StringLiteral) visit(context.string())).getValue(),
                Optional.ofNullable(context.sign)
                        .map(AstBuilder::getIntervalSign)
                        .orElse(IntervalLiteral.Sign.POSITIVE),
                getIntervalFieldType((Token) context.from.getChild(0).getPayload()),
                Optional.ofNullable(context.to)
                        .map((x) -> x.getChild(0).getPayload())
                        .map(Token.class::cast)
                        .map(AstBuilder::getIntervalFieldType));
    }

    @Override
    public Node visitParameter(SqlFlowStatementParser.ParameterContext context) {
        Parameter parameter = new Parameter(getLocation(context), parameterPosition);
        parameterPosition++;
        return parameter;
    }

    // ***************** arguments *****************

    @Override
    public Node visitQualifiedArgument(SqlFlowStatementParser.QualifiedArgumentContext context) {
        return new PathElement(getLocation(context), (Identifier) visit(context.identifier(0)), (Identifier) visit(context.identifier(1)));
    }

    @Override
    public Node visitUnqualifiedArgument(SqlFlowStatementParser.UnqualifiedArgumentContext context) {
        return new PathElement(getLocation(context), (Identifier) visit(context.identifier()));
    }

    @Override
    public Node visitPathSpecification(SqlFlowStatementParser.PathSpecificationContext context) {
        return new PathSpecification(getLocation(context), visit(context.pathElement(), PathElement.class));
    }

    @Override
    public Node visitRowType(SqlFlowStatementParser.RowTypeContext context) {
        List<RowDataType.Field> fields = context.rowField().stream()
                .map(this::visit)
                .map(RowDataType.Field.class::cast)
                .collect(toImmutableList());

        return new RowDataType(getLocation(context), fields);
    }

    @Override
    public Node visitRowField(SqlFlowStatementParser.RowFieldContext context) {
        return new RowDataType.Field(
                getLocation(context),
                visitIfPresent(context.identifier(), Identifier.class),
                (DataType) visit(context.type()));
    }

    @Override
    public Node visitGenericType(SqlFlowStatementParser.GenericTypeContext context) {
        List<DataTypeParameter> parameters = context.typeParameter().stream()
                .map(this::visit)
                .map(DataTypeParameter.class::cast)
                .collect(toImmutableList());

        return new GenericDataType(getLocation(context), (Identifier) visit(context.identifier()), parameters);
    }

    @Override
    public Node visitTypeParameter(SqlFlowStatementParser.TypeParameterContext context) {
        if (context.INTEGER_VALUE() != null) {
            return new NumericParameter(getLocation(context), context.getText());
        }

        return new TypeParameter((DataType) visit(context.type()));
    }

    @Override
    public Node visitIntervalType(SqlFlowStatementParser.IntervalTypeContext context) {
        String from = context.from.getText();
        String to = getTextIfPresent(context.to)
                .orElse(from);

        return new IntervalDayTimeDataType(
                getLocation(context),
                IntervalDayTimeDataType.Field.valueOf(from.toUpperCase(ENGLISH)),
                IntervalDayTimeDataType.Field.valueOf(to.toUpperCase(ENGLISH)));
    }

    @Override
    public Node visitDateTimeType(SqlFlowStatementParser.DateTimeTypeContext context) {
        DateTimeDataType.Type type;

        if (context.base.getType() == TIME) {
            type = DateTimeDataType.Type.TIME;
        } else if (context.base.getType() == TIMESTAMP) {
            type = DateTimeDataType.Type.TIMESTAMP;
        } else {
            throw new ParsingException("Unexpected datetime type: " + context.getText());
        }

        return new DateTimeDataType(
                getLocation(context),
                type,
                context.WITH() != null,
                visitIfPresent(context.precision, DataTypeParameter.class));
    }

    @Override
    public Node visitDoublePrecisionType(SqlFlowStatementParser.DoublePrecisionTypeContext context) {
        return new GenericDataType(
                getLocation(context),
                new Identifier(getLocation(context.DOUBLE()), context.DOUBLE().getText(), false),
                ImmutableList.of());
    }

    @Override
    public Node visitLegacyArrayType(SqlFlowStatementParser.LegacyArrayTypeContext context) {
        return new GenericDataType(
                getLocation(context),
                new Identifier(getLocation(context.ARRAY()), context.ARRAY().getText(), false),
                ImmutableList.of(new TypeParameter((DataType) visit(context.type()))));
    }

    @Override
    public Node visitLegacyMapType(SqlFlowStatementParser.LegacyMapTypeContext context) {
        return new GenericDataType(
                getLocation(context),
                new Identifier(getLocation(context.MAP()), context.MAP().getText(), false),
                ImmutableList.of(
                        new TypeParameter((DataType) visit(context.keyType)),
                        new TypeParameter((DataType) visit(context.valueType))));
    }

    @Override
    public Node visitArrayType(SqlFlowStatementParser.ArrayTypeContext context) {
        if (context.INTEGER_VALUE() != null) {
            throw new UnsupportedOperationException("Explicit array size not supported");
        }

        return new GenericDataType(
                getLocation(context),
                new Identifier(getLocation(context.ARRAY()), context.ARRAY().getText(), false),
                ImmutableList.of(new TypeParameter((DataType) visit(context.type()))));
    }

    @Override
    public Node visitQueryPeriod(SqlFlowStatementParser.QueryPeriodContext context) {
        QueryPeriod.RangeType type = getRangeType((Token) context.rangeType().getChild(0).getPayload());
        Expression marker = (Expression) visit(context.valueExpression());
        return new QueryPeriod(getLocation(context), type, marker);
    }

    // ***************** helpers *****************

    @Override
    protected Node defaultResult() {
        return null;
    }

    @Override
    protected Node aggregateResult(Node aggregate, Node nextResult) {
        if (nextResult == null) {
            throw new UnsupportedOperationException("not yet implemented");
        }

        if (aggregate == null) {
            return nextResult;
        }

        throw new UnsupportedOperationException("not yet implemented");
    }

    private enum UnicodeDecodeState {
        EMPTY,
        ESCAPED,
        UNICODE_SEQUENCE
    }

    private static String decodeUnicodeLiteral(SqlFlowStatementParser.UnicodeStringLiteralContext context) {
        char escape;
        if (context.UESCAPE() != null) {
            String escapeString = unquote(context.STRING().getText());
            check(!escapeString.isEmpty(), "Empty Unicode escape character", context);
            check(escapeString.length() == 1, "Invalid Unicode escape character: " + escapeString, context);
            escape = escapeString.charAt(0);
            check(isValidUnicodeEscape(escape), "Invalid Unicode escape character: " + escapeString, context);
        } else {
            escape = '\\';
        }

        String rawContent = unquote(context.UNICODE_STRING().getText().substring(2));
        StringBuilder unicodeStringBuilder = new StringBuilder();
        StringBuilder escapedCharacterBuilder = new StringBuilder();
        int charactersNeeded = 0;
        UnicodeDecodeState state = UnicodeDecodeState.EMPTY;
        for (int i = 0; i < rawContent.length(); i++) {
            char ch = rawContent.charAt(i);
            switch (state) {
                case EMPTY:
                    if (ch == escape) {
                        state = UnicodeDecodeState.ESCAPED;
                    } else {
                        unicodeStringBuilder.append(ch);
                    }
                    break;
                case ESCAPED:
                    if (ch == escape) {
                        unicodeStringBuilder.append(escape);
                        state = UnicodeDecodeState.EMPTY;
                    } else if (ch == '+') {
                        state = UnicodeDecodeState.UNICODE_SEQUENCE;
                        charactersNeeded = 6;
                    } else if (isHexDigit(ch)) {
                        state = UnicodeDecodeState.UNICODE_SEQUENCE;
                        charactersNeeded = 4;
                        escapedCharacterBuilder.append(ch);
                    } else {
                        throw parseError("Invalid hexadecimal digit: " + ch, context);
                    }
                    break;
                case UNICODE_SEQUENCE:
                    check(isHexDigit(ch), "Incomplete escape sequence: " + escapedCharacterBuilder.toString(), context);
                    escapedCharacterBuilder.append(ch);
                    if (charactersNeeded == escapedCharacterBuilder.length()) {
                        String currentEscapedCode = escapedCharacterBuilder.toString();
                        escapedCharacterBuilder.setLength(0);
                        int codePoint = Integer.parseInt(currentEscapedCode, 16);
                        check(Character.isValidCodePoint(codePoint), "Invalid escaped character: " + currentEscapedCode, context);
                        if (Character.isSupplementaryCodePoint(codePoint)) {
                            unicodeStringBuilder.appendCodePoint(codePoint);
                        } else {
                            char currentCodePoint = (char) codePoint;
                            check(!Character.isSurrogate(currentCodePoint), format("Invalid escaped character: %s. Escaped character is a surrogate. Use '\\+123456' instead.", currentEscapedCode), context);
                            unicodeStringBuilder.append(currentCodePoint);
                        }
                        state = UnicodeDecodeState.EMPTY;
                        charactersNeeded = -1;
                    } else {
                        check(charactersNeeded > escapedCharacterBuilder.length(), "Unexpected escape sequence length: " + escapedCharacterBuilder.length(), context);
                    }
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }

        check(state == UnicodeDecodeState.EMPTY, "Incomplete escape sequence: " + escapedCharacterBuilder.toString(), context);
        return unicodeStringBuilder.toString();
    }

    private <T> Optional<T> visitIfPresent(ParserRuleContext context, Class<T> clazz) {
        return Optional.ofNullable(context)
                .map(this::visit)
                .map(clazz::cast);
    }

    private <T> List<T> visit(List<? extends ParserRuleContext> contexts, Class<T> clazz) {
        return contexts.stream()
                .map(this::visit)
                .map(clazz::cast)
                .collect(toList());
    }

    private static String unquote(String value) {
        return value.substring(1, value.length() - 1)
                .replace("''", "'");
    }

    private static LikeClause.PropertiesOption getPropertiesOption(Token token) {
        switch (token.getType()) {
            case SqlFlowStatementLexer.INCLUDING:
                return LikeClause.PropertiesOption.INCLUDING;
            case SqlFlowStatementLexer.EXCLUDING:
                return LikeClause.PropertiesOption.EXCLUDING;
        }
        throw new IllegalArgumentException("Unsupported LIKE option type: " + token.getText());
    }

    private QualifiedName getQualifiedName(SqlFlowStatementParser.QualifiedNameContext context) {
        return QualifiedName.of(visit(context.identifier(), Identifier.class));
    }

    private static boolean isDistinct(SqlFlowStatementParser.SetQuantifierContext setQuantifier) {
        return setQuantifier != null && setQuantifier.DISTINCT() != null;
    }

    private static boolean isHexDigit(char c) {
        return ((c >= '0') && (c <= '9')) ||
                ((c >= 'A') && (c <= 'F')) ||
                ((c >= 'a') && (c <= 'f'));
    }

    private static boolean isValidUnicodeEscape(char c) {
        return c < 0x7F && c > 0x20 && !isHexDigit(c) && c != '"' && c != '+' && c != '\'';
    }

    private static Optional<String> getTextIfPresent(ParserRuleContext context) {
        return Optional.ofNullable(context)
                .map(ParseTree::getText);
    }

    private Optional<Identifier> getIdentifierIfPresent(ParserRuleContext context) {
        return Optional.ofNullable(context).map(c -> (Identifier) visit(c));
    }

    private static ArithmeticBinaryExpression.Operator getArithmeticBinaryOperator(Token operator) {
        switch (operator.getType()) {
            case SqlFlowStatementLexer.PLUS:
                return ArithmeticBinaryExpression.Operator.ADD;
            case SqlFlowStatementLexer.MINUS:
                return ArithmeticBinaryExpression.Operator.SUBTRACT;
            case SqlFlowStatementLexer.ASTERISK:
                return ArithmeticBinaryExpression.Operator.MULTIPLY;
            case SqlFlowStatementLexer.SLASH:
                return ArithmeticBinaryExpression.Operator.DIVIDE;
            case SqlFlowStatementLexer.PERCENT:
                return ArithmeticBinaryExpression.Operator.MODULUS;
        }

        throw new UnsupportedOperationException("Unsupported operator: " + operator.getText());
    }

    private static ComparisonExpression.Operator getComparisonOperator(Token symbol) {
        switch (symbol.getType()) {
            case SqlFlowStatementLexer.EQ:
                return ComparisonExpression.Operator.EQUAL;
            case SqlFlowStatementLexer.NEQ:
                return ComparisonExpression.Operator.NOT_EQUAL;
            case SqlFlowStatementLexer.LT:
                return ComparisonExpression.Operator.LESS_THAN;
            case SqlFlowStatementLexer.LTE:
                return ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
            case SqlFlowStatementLexer.GT:
                return ComparisonExpression.Operator.GREATER_THAN;
            case SqlFlowStatementLexer.GTE:
                return ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
        }

        throw new IllegalArgumentException("Unsupported operator: " + symbol.getText());
    }

    private static CurrentTime.Function getDateTimeFunctionType(Token token) {
        switch (token.getType()) {
            case SqlFlowStatementLexer.CURRENT_DATE:
                return CurrentTime.Function.DATE;
            case SqlFlowStatementLexer.CURRENT_TIME:
                return CurrentTime.Function.TIME;
            case SqlFlowStatementLexer.CURRENT_TIMESTAMP:
                return CurrentTime.Function.TIMESTAMP;
            case SqlFlowStatementLexer.LOCALTIME:
                return CurrentTime.Function.LOCALTIME;
            case SqlFlowStatementLexer.LOCALTIMESTAMP:
                return CurrentTime.Function.LOCALTIMESTAMP;
        }

        throw new IllegalArgumentException("Unsupported special function: " + token.getText());
    }

    private static IntervalLiteral.IntervalField getIntervalFieldType(Token token) {
        switch (token.getType()) {
            case SqlFlowStatementLexer.YEAR:
                return IntervalLiteral.IntervalField.YEAR;
            case SqlFlowStatementLexer.MONTH:
                return IntervalLiteral.IntervalField.MONTH;
            case SqlFlowStatementLexer.DAY:
                return IntervalLiteral.IntervalField.DAY;
            case SqlFlowStatementLexer.HOUR:
                return IntervalLiteral.IntervalField.HOUR;
            case SqlFlowStatementLexer.MINUTE:
                return IntervalLiteral.IntervalField.MINUTE;
            case SqlFlowStatementLexer.SECOND:
                return IntervalLiteral.IntervalField.SECOND;
        }

        throw new IllegalArgumentException("Unsupported interval field: " + token.getText());
    }

    private static IntervalLiteral.Sign getIntervalSign(Token token) {
        switch (token.getType()) {
            case SqlFlowStatementLexer.MINUS:
                return IntervalLiteral.Sign.NEGATIVE;
            case SqlFlowStatementLexer.PLUS:
                return IntervalLiteral.Sign.POSITIVE;
        }

        throw new IllegalArgumentException("Unsupported sign: " + token.getText());
    }

    private static WindowFrame.Type getFrameType(Token type) {
        switch (type.getType()) {
            case SqlFlowStatementLexer.RANGE:
                return WindowFrame.Type.RANGE;
            case SqlFlowStatementLexer.ROWS:
                return WindowFrame.Type.ROWS;
            case SqlFlowStatementLexer.GROUPS:
                return WindowFrame.Type.GROUPS;
        }

        throw new IllegalArgumentException("Unsupported frame type: " + type.getText());
    }

    private static FrameBound.Type getBoundedFrameBoundType(Token token) {
        switch (token.getType()) {
            case SqlFlowStatementLexer.PRECEDING:
                return FrameBound.Type.PRECEDING;
            case SqlFlowStatementLexer.FOLLOWING:
                return FrameBound.Type.FOLLOWING;
        }

        throw new IllegalArgumentException("Unsupported bound type: " + token.getText());
    }

    private static FrameBound.Type getUnboundedFrameBoundType(Token token) {
        switch (token.getType()) {
            case SqlFlowStatementLexer.PRECEDING:
                return FrameBound.Type.UNBOUNDED_PRECEDING;
            case SqlFlowStatementLexer.FOLLOWING:
                return FrameBound.Type.UNBOUNDED_FOLLOWING;
        }

        throw new IllegalArgumentException("Unsupported bound type: " + token.getText());
    }

    private static SampledRelation.Type getSamplingMethod(Token token) {
        switch (token.getType()) {
            case SqlFlowStatementLexer.BERNOULLI:
                return SampledRelation.Type.BERNOULLI;
            case SqlFlowStatementLexer.SYSTEM:
                return SampledRelation.Type.SYSTEM;
        }

        throw new IllegalArgumentException("Unsupported sampling method: " + token.getText());
    }

    private static SortItem.NullOrdering getNullOrderingType(Token token) {
        switch (token.getType()) {
            case SqlFlowStatementLexer.FIRST:
                return SortItem.NullOrdering.FIRST;
            case SqlFlowStatementLexer.LAST:
                return SortItem.NullOrdering.LAST;
        }

        throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
    }

    private static SortItem.Ordering getOrderingType(Token token) {
        switch (token.getType()) {
            case SqlFlowStatementLexer.ASC:
                return SortItem.Ordering.ASCENDING;
            case SqlFlowStatementLexer.DESC:
                return SortItem.Ordering.DESCENDING;
        }

        throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
    }

    private static QuantifiedComparisonExpression.Quantifier getComparisonQuantifier(Token symbol) {
        switch (symbol.getType()) {
            case SqlFlowStatementLexer.ALL:
                return QuantifiedComparisonExpression.Quantifier.ALL;
            case SqlFlowStatementLexer.ANY:
                return QuantifiedComparisonExpression.Quantifier.ANY;
            case SqlFlowStatementLexer.SOME:
                return QuantifiedComparisonExpression.Quantifier.SOME;
        }

        throw new IllegalArgumentException("Unsupported quantifier: " + symbol.getText());
    }

    private List<Identifier> getIdentifiers(List<SqlFlowStatementParser.IdentifierContext> identifiers) {
        return identifiers.stream().map(context -> (Identifier) visit(context)).collect(toList());
    }

    private static void check(boolean condition, String message, ParserRuleContext context) {
        if (!condition) {
            throw parseError(message, context);
        }
    }

    public static NodeLocation getLocation(TerminalNode terminalNode) {
        requireNonNull(terminalNode, "terminalNode is null");
        return getLocation(terminalNode.getSymbol());
    }

    public static NodeLocation getLocation(ParserRuleContext parserRuleContext) {
        requireNonNull(parserRuleContext, "parserRuleContext is null");
        return getLocation(parserRuleContext.getStart());
    }

    public static NodeLocation getLocation(Token token) {
        requireNonNull(token, "token is null");
        return new NodeLocation(token.getLine(), token.getCharPositionInLine() + 1,
                token.getStartIndex(), token.getTokenIndex());
    }

    private static ParsingException parseError(String message, ParserRuleContext context) {
        return new ParsingException(message, null, context.getStart().getLine(), context.getStart().getCharPositionInLine() + 1);
    }

    private static QueryPeriod.RangeType getRangeType(Token token) {
        switch (token.getType()) {
            case TIMESTAMP:
                return QueryPeriod.RangeType.TIMESTAMP;
            case VERSION:
                return QueryPeriod.RangeType.VERSION;
            case SYSTEM_TIME:
                return QueryPeriod.RangeType.SYSTEM_TIME;
        }
        throw new IllegalArgumentException("Unsupported query period range type: " + token.getText());
    }
}
