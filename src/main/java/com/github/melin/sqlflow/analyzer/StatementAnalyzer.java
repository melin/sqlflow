package com.github.melin.sqlflow.analyzer;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.SqlFlowException;
import com.github.melin.sqlflow.analyzer.Analysis.SelectExpression;
import com.github.melin.sqlflow.analyzer.Analysis.SourceColumn;
import com.github.melin.sqlflow.metadata.*;
import com.github.melin.sqlflow.parser.ParsingException;
import com.github.melin.sqlflow.parser.SqlParser;
import com.github.melin.sqlflow.tree.*;
import com.github.melin.sqlflow.tree.expression.*;
import com.github.melin.sqlflow.tree.group.*;
import com.github.melin.sqlflow.tree.join.Join;
import com.github.melin.sqlflow.tree.join.JoinCriteria;
import com.github.melin.sqlflow.tree.join.JoinOn;
import com.github.melin.sqlflow.tree.join.JoinUsing;
import com.github.melin.sqlflow.tree.literal.LongLiteral;
import com.github.melin.sqlflow.tree.relation.Table;
import com.github.melin.sqlflow.tree.relation.*;
import com.github.melin.sqlflow.tree.statement.*;
import com.github.melin.sqlflow.tree.window.*;
import com.github.melin.sqlflow.tree.window.rowPattern.PatternRecognitionRelation;
import com.github.melin.sqlflow.tree.window.rowPattern.RowPattern;
import com.github.melin.sqlflow.type.ArrayType;
import com.github.melin.sqlflow.type.MapType;
import com.github.melin.sqlflow.type.RowType;
import com.github.melin.sqlflow.type.Type;
import com.google.common.collect.*;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.melin.sqlflow.analyzer.ExpressionTreeUtils.*;
import static com.github.melin.sqlflow.analyzer.Scope.BasisType.TABLE;
import static com.github.melin.sqlflow.analyzer.SemanticExceptions.semanticException;
import static com.github.melin.sqlflow.metadata.MetadataUtil.createQualifiedObjectName;
import static com.github.melin.sqlflow.tree.literal.BooleanLiteral.TRUE_LITERAL;
import static com.github.melin.sqlflow.util.AstUtils.preOrder;
import static com.github.melin.sqlflow.util.NodeUtils.getSortItemsFromOrderBy;
import static com.google.common.base.Preconditions.*;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getLast;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/23 4:26 PM
 */
public class StatementAnalyzer {

    private static final Set<String> WINDOW_VALUE_FUNCTIONS = ImmutableSet.of("lead", "lag", "first_value", "last_value", "nth_value");

    private final MetadataService metadataService;

    private final Analysis analysis;

    private final SqlParser sqlParser;

    public StatementAnalyzer(Analysis analysis, MetadataService metadataService, SqlParser sqlParser) {
        this.analysis = requireNonNull(analysis, "analysis is null");
        this.metadataService = requireNonNull(metadataService, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
    }

    public Scope analyze(Node node, Scope outerQueryScope) {
        return analyze(node, Optional.of(outerQueryScope));
    }

    public Scope analyze(Node node, Optional<Scope> outerQueryScope) {
        return new Visitor(outerQueryScope, Optional.empty()).process(node, Optional.empty());
    }

    private Scope analyzeForUpdate(com.github.melin.sqlflow.tree.relation.Table table, Optional<Scope> outerQueryScope, UpdateKind updateKind) {
        return new Visitor(outerQueryScope, Optional.of(updateKind)).process(table, Optional.empty());
    }

    private enum UpdateKind {
        DELETE, UPDATE,
    }

    private final class Visitor extends AstVisitor<Scope, Optional<Scope>> {

        private final Optional<Scope> outerQueryScope;
        private final Optional<UpdateKind> updateKind;

        public Visitor(Optional<Scope> outerQueryScope, Optional<UpdateKind> updateKind) {
            this.outerQueryScope = requireNonNull(outerQueryScope, "outerQueryScope is null");
            this.updateKind = requireNonNull(updateKind, "updateKind is null");
        }

        @Override
        public Scope process(Node node, Optional<Scope> scope) {
            Scope returnScope = super.process(node, scope);
            checkState(returnScope.getOuterQueryParent().equals(outerQueryScope), "result scope should have outer query scope equal with parameter outer query scope");
            scope.ifPresent(value -> checkState(hasScopeAsLocalParent(returnScope, value), "return scope should have context scope as one of its ancestors"));
            return returnScope;
        }

        public Scope process(Node node, Scope scope) {
            return process(node, Optional.of(scope));
        }

        @Override
        public Scope visitNode(Node node, Optional<Scope> scope) {
            throw new IllegalStateException("Unsupported node type: " + node.getClass().getName());
        }

        @Override
        public Scope visitInsert(Insert insert, Optional<Scope> scope) {
            QualifiedObjectName targetTable = createQualifiedObjectName(metadataService, insert, insert.getTarget());
            Scope queryScope = analyze(insert.getQuery(), createScope(scope));

            Optional<SchemaTable> tableSchema = metadataService.getTableSchema(targetTable);

            if (!tableSchema.isPresent()) {
                throw new ParsingException("table " + targetTable + " metadata not exists");
            }

            List<String> tableColumns = tableSchema.get().getColumns();
            List<String> insertColumns;
            if (insert.getColumns().isPresent()) {
                insertColumns = insert.getColumns().get().stream().map(Identifier::getValue).collect(toImmutableList());

                Set<String> columnNames = new HashSet<>();
                for (String insertColumn : insertColumns) {
                    if (!tableColumns.contains(insertColumn)) {
                        throw semanticException(insert, "Insert column name does not exist in target table: %s", insertColumn);
                    }
                    if (!columnNames.add(insertColumn)) {
                        throw semanticException(insert, "Insert column name is specified more than once: %s", insertColumn);
                    }
                }
            } else {
                insertColumns = tableColumns;
            }

            Stream<String> columnStream = insertColumns.stream();

            analysis.setUpdateType("INSERT");
            analysis.setUpdateTarget(
                    targetTable,
                    Optional.empty(),
                    Optional.of(Streams.zip(
                                    columnStream,
                                    queryScope.getRelationType().getVisibleFields().stream(),
                                    (column, field) -> new OutputColumn(column, analysis.getSourceColumns(field)))
                            .collect(toImmutableList())));

            return createAndAssignScope(insert, scope);
        }

        @Override
        public Scope visitCreateTableAsSelect(CreateTableAsSelect node, Optional<Scope> scope) {
            // turn this into a query that has a new table writer node on top.
            QualifiedObjectName targetTable = createQualifiedObjectName(metadataService, node, node.getName());

            Optional<SchemaTable> tableSchema = metadataService.getTableSchema(targetTable);
            if (tableSchema.isPresent()) {
                if (node.isNotExists()) {
                    analysis.setUpdateType("CREATE TABLE");
                    analysis.setUpdateTarget(targetTable, Optional.empty(), Optional.of(ImmutableList.of()));
                    return createAndAssignScope(node, scope);
                }
                throw semanticException(node, "Destination table '%s' already exists", targetTable);
            }

            // analyze the query that creates the table
            Scope queryScope = analyze(node.getQuery(), createScope(scope));

            // analyze target table columns and column aliases
            ImmutableList.Builder<OutputColumn> outputColumns = ImmutableList.builder();
            if (node.getColumnAliases().isPresent()) {
                validateColumnAliases(node.getColumnAliases().get(), queryScope.getRelationType().getVisibleFieldCount());

                int aliasPosition = 0;
                for (Field field : queryScope.getRelationType().getVisibleFields()) {
                    String columnName = node.getColumnAliases().get().get(aliasPosition).getValue();
                    outputColumns.add(new OutputColumn(columnName, analysis.getSourceColumns(field)));
                    aliasPosition++;
                }
            } else {
                queryScope.getRelationType().getVisibleFields().stream().map(this::createOutputColumn).forEach(outputColumns::add);
            }

            analysis.setUpdateType("CREATE TABLE");
            analysis.setUpdateTarget(targetTable, Optional.empty(), Optional.of(outputColumns.build()));

            return createAndAssignScope(node, scope);
        }

        @Override
        public Scope visitCreateView(CreateView node, Optional<Scope> scope) {
            QualifiedObjectName viewName = createQualifiedObjectName(metadataService, node, node.getName());

            // analyze the query that creates the view
            StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadataService, sqlParser);

            Scope queryScope = analyzer.analyze(node.getQuery(), scope);

            analysis.setUpdateType("CREATE VIEW");
            analysis.setUpdateTarget(viewName, Optional.empty(), Optional.of(queryScope.getRelationType().getVisibleFields().stream().map(this::createOutputColumn).collect(toImmutableList())));

            return createAndAssignScope(node, scope);
        }

        @Override
        public Scope visitTable(com.github.melin.sqlflow.tree.relation.Table table, Optional<Scope> scope) {
            if (!table.getName().getPrefix().isPresent()) {
                // is this a reference to a WITH query?
                Optional<WithQuery> withQuery = createScope(scope).getNamedQuery(table.getName().getSuffix());
                if (withQuery.isPresent()) {
                    return createScopeForCommonTableExpression(table, scope, withQuery.get());
                }
                // is this a recursive reference in expandable WITH query? If so, there's base scope recorded.
                Optional<Scope> expandableBaseScope = analysis.getExpandableBaseScope(table);
                if (expandableBaseScope.isPresent()) {
                    Scope baseScope = expandableBaseScope.get();
                    // adjust local and outer parent scopes accordingly to the local context of the recursive reference
                    Scope resultScope = scopeBuilder(scope).withRelationType(baseScope.getRelationId(), baseScope.getRelationType()).build();
                    analysis.setScope(table, resultScope);
                    return resultScope;
                }
            }

            QualifiedObjectName name = createQualifiedObjectName(metadataService, table, table.getName());

            // This could be a reference to a logical view or a table
            Optional<ViewDefinition> optionalView = metadataService.getView(name);
            if (optionalView.isPresent()) {
                return createScopeForView(table, name, scope, optionalView.get());
            }

            Optional<SchemaTable> schemaTable = metadataService.getTableSchema(name);
            if (!schemaTable.isPresent()) {
                throw new ParsingException("table " + name + " metadata not exists");
            }

            ImmutableList.Builder<Field> fields = ImmutableList.builder();
            fields.addAll(analyzeTableOutputFields(table, name, schemaTable.get()));

            analysis.addOriginTable(name, table.getLocation().get());

            if (updateKind.isPresent()) {
                //@TODO
            }

            List<Field> outputFields = fields.build();

            Scope tableScope = createAndAssignScope(table, scope, outputFields);

            if (updateKind.isPresent()) {
                FieldReference reference = new FieldReference(outputFields.size() - 1);
            }

            return tableScope;
        }

        private List<Field> analyzeTableOutputFields(com.github.melin.sqlflow.tree.relation.Table table, QualifiedObjectName tableName, SchemaTable schemaTable) {
            // TODO: discover columns lazily based on where they are needed (to support connectors that can't enumerate all tables)
            ImmutableList.Builder<Field> fields = ImmutableList.builder();
            for (String column : schemaTable.getColumns()) {
                Field field = Field.newQualified(table.getName(), Optional.of(column), Optional.of(tableName), Optional.of(column), false);
                fields.add(field);
                analysis.addSourceColumns(field, ImmutableSet.of(new SourceColumn(tableName, column)));
            }
            return fields.build();
        }

        private Scope createScopeForCommonTableExpression(com.github.melin.sqlflow.tree.relation.Table table, Optional<Scope> scope, WithQuery withQuery) {
            Query query = withQuery.getQuery();
            analysis.registerNamedQuery(table, query);

            // re-alias the fields with the name assigned to the query in the WITH declaration
            RelationType queryDescriptor = analysis.getOutputDescriptor(query);

            List<Field> fields;
            Optional<List<Identifier>> columnNames = withQuery.getColumnNames();
            if (columnNames.isPresent()) {
                // if columns are explicitly aliased -> WITH cte(alias1, alias2 ...)
                checkState(columnNames.get().size() == queryDescriptor.getVisibleFieldCount(), "mismatched aliases");
                ImmutableList.Builder<Field> fieldBuilder = ImmutableList.builder();
                Iterator<Identifier> aliases = columnNames.get().iterator();
                for (int i = 0; i < queryDescriptor.getAllFieldCount(); i++) {
                    Field inputField = queryDescriptor.getFieldByIndex(i);
                    Field field = Field.newQualified(QualifiedName.of(table.getName().getSuffix()), Optional.of(aliases.next().getValue()), inputField.getOriginTable(), inputField.getOriginColumnName(), inputField.isAliased());
                    fieldBuilder.add(field);
                    analysis.addSourceColumns(field, analysis.getSourceColumns(inputField));
                }
                fields = fieldBuilder.build();
            } else {
                ImmutableList.Builder<Field> fieldBuilder = ImmutableList.builder();
                for (int i = 0; i < queryDescriptor.getAllFieldCount(); i++) {
                    Field inputField = queryDescriptor.getFieldByIndex(i);
                    Field field = Field.newQualified(QualifiedName.of(table.getName().getSuffix()), inputField.getName(), inputField.getOriginTable(), inputField.getOriginColumnName(), inputField.isAliased());
                    fieldBuilder.add(field);
                    analysis.addSourceColumns(field, analysis.getSourceColumns(inputField));
                }
                fields = fieldBuilder.build();
            }

            return createAndAssignScope(table, scope, fields);
        }

        private Scope createScopeForView(com.github.melin.sqlflow.tree.relation.Table table, QualifiedObjectName name, Optional<Scope> scope, ViewDefinition view) {
            return createScopeForView(table, name, scope, view.getOriginalSql(), view.getCatalog(), view.getSchema(), view.getColumns());
        }

        private Scope createScopeForView(com.github.melin.sqlflow.tree.relation.Table table, QualifiedObjectName name, Optional<Scope> scope, String originalSql, Optional<String> catalog, Optional<String> schema, List<ViewColumn> columns) {
            Statement statement = analysis.getStatement();
            if (statement instanceof CreateView) {
                CreateView viewStatement = (CreateView) statement;
                QualifiedObjectName viewNameFromStatement = createQualifiedObjectName(metadataService, viewStatement, viewStatement.getName());
                if (viewStatement.isReplace() && viewNameFromStatement.equals(name)) {
                    throw semanticException(table, "Statement would create a recursive view");
                }
            }
            if (statement instanceof CreateMaterializedView) {
                CreateMaterializedView viewStatement = (CreateMaterializedView) statement;
                QualifiedObjectName viewNameFromStatement = createQualifiedObjectName(metadataService, viewStatement, viewStatement.getName());
                if (viewStatement.isReplace() && viewNameFromStatement.equals(name)) {
                    throw semanticException(table, "Statement would create a recursive materialized view");
                }
            }
            if (analysis.hasTableInView(table)) {
                throw semanticException(table, "View is recursive");
            }

            Query query = parseView(originalSql, name, table);
            analysis.registerNamedQuery(table, query);
            analysis.registerTableForView(table);
            analyzeView(query, name, catalog, schema, table);
            analysis.unregisterTableForView();

            // Derive the type of the view from the stored definition, not from the analysis of the underlying query.
            // This is needed in case the underlying table(s) changed and the query in the view now produces types that
            // are implicitly coercible to the declared view types.
            List<Field> outputFields = columns.stream().map(column -> Field.newQualified(table.getName(),
                    Optional.of(column.getName()), Optional.of(name),
                    Optional.of(column.getName()), false)).collect(toImmutableList());

            outputFields.forEach(field -> analysis.addSourceColumns(field, ImmutableSet.of(new SourceColumn(name, field.getName().get()))));
            return createAndAssignScope(table, scope, outputFields);
        }

        private RelationType analyzeView(Query query, QualifiedObjectName name, Optional<String> catalog, Optional<String> schema, com.github.melin.sqlflow.tree.relation.Table node) {
            try {

                StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadataService, sqlParser);
                Scope queryScope = analyzer.analyze(query, Scope.create());
                return queryScope.getRelationType().withAlias(name.getObjectName(), null);
            } catch (RuntimeException e) {
                throw semanticException(node, e, "Failed analyzing stored view '%s': %s", name, e.getMessage());
            }
        }

        private Query parseView(String view, QualifiedObjectName name, Node node) {
            try {
                return (Query) sqlParser.createStatement(view);
            } catch (ParsingException e) {
                throw semanticException(node, e, "Failed parsing stored view '%s': %s", name, e.getMessage());
            }
        }

        @Override
        public Scope visitQuery(Query node, Optional<Scope> scope) {
            Scope withScope = analyzeWith(node, scope);
            Scope queryBodyScope = process(node.getQueryBody(), withScope);

            List<Expression> orderByExpressions = emptyList();
            if (node.getOrderBy().isPresent()) {
                orderByExpressions = analyzeOrderBy(node, getSortItemsFromOrderBy(node.getOrderBy()), queryBodyScope);
            }
            analysis.setOrderByExpressions(node, orderByExpressions);

            // Input fields == Output fields
            analysis.setSelectExpressions(node, descriptorToFields(queryBodyScope).stream()
                    .map(expression -> new SelectExpression(expression, Optional.empty())).collect(toImmutableList()));

            Scope queryScope = Scope.builder().withParent(withScope)
                    .withRelationType(RelationId.of(node), queryBodyScope.getRelationType()).build();

            analysis.setScope(node, queryScope);
            return queryScope;
        }

        private List<Expression> analyzeOrderBy(Node node, List<SortItem> sortItems, Scope orderByScope) {
            ImmutableList.Builder<Expression> orderByFieldsBuilder = ImmutableList.builder();

            for (SortItem item : sortItems) {
                Expression expression = item.getSortKey();

                if (expression instanceof LongLiteral) {
                    // this is an ordinal in the output tuple

                    long ordinal = ((LongLiteral) expression).getValue();
                    if (ordinal < 1 || ordinal > orderByScope.getRelationType().getVisibleFieldCount()) {
                        throw semanticException(expression, "ORDER BY position %s is not in select list", ordinal);
                    }

                    expression = new FieldReference(toIntExact(ordinal - 1));
                }

                ExpressionAnalyzer.analyzeExpression(
                        orderByScope,
                        analysis,
                        metadataService,
                        sqlParser,
                        expression);

                Type type = analysis.getType(expression);
                if (!type.isOrderable()) {
                    throw semanticException(node, "Type %s is not orderable, and therefore cannot be used in ORDER BY: %s", type, expression);
                }

                orderByFieldsBuilder.add(expression);
            }

            return orderByFieldsBuilder.build();
        }

        @Override
        public Scope visitUnnest(Unnest node, Optional<Scope> scope) {
            ImmutableMap.Builder<NodeRef<Expression>, List<Field>> mappings = ImmutableMap.builder();

            ImmutableList.Builder<Field> outputFields = ImmutableList.builder();
            for (Expression expression : node.getExpressions()) {
                List<Field> expressionOutputs = new ArrayList<>();

                ExpressionAnalysis expressionAnalysis = ExpressionAnalyzer.analyzeExpression(createScope(scope), analysis, metadataService, sqlParser, expression);
                Type expressionType = expressionAnalysis.getType(expression);
                if (expressionType instanceof ArrayType) {
                    /*Type elementType = ((ArrayType) expressionType).getElementType();
                    if (elementType instanceof RowType) {
                        ((RowType) elementType).getFields().stream()
                                .map(field -> Field.newUnqualified(field.getName(), field.getType()))
                                .forEach(expressionOutputs::add);
                    } else {
                        expressionOutputs.add(Field.newUnqualified(Optional.empty(), elementType));
                    }*/
                } else if (expressionType instanceof MapType) {
                    /*expressionOutputs.add(Field.newUnqualified(Optional.empty(), ((MapType) expressionType).getKeyType()));
                    expressionOutputs.add(Field.newUnqualified(Optional.empty(), ((MapType) expressionType).getValueType()));*/
                } else {
                    throw new SqlFlowException("Cannot unnest type: " + expressionType);
                }

                outputFields.addAll(expressionOutputs);
                mappings.put(NodeRef.of(expression), expressionOutputs);
            }

            Optional<Field> ordinalityField = Optional.empty();
            if (node.isWithOrdinality()) {
                ordinalityField = Optional.of(Field.newUnqualified(Optional.empty()));
            }

            ordinalityField.ifPresent(outputFields::add);

            return createAndAssignScope(node, scope, outputFields.build());
        }

        private Scope analyzeWith(Query node, Optional<Scope> scope) {
            if (!node.getWith().isPresent()) {
                return createScope(scope);
            }

            // analyze WITH clause
            With with = node.getWith().get();
            Scope.Builder withScopeBuilder = scopeBuilder(scope);

            for (WithQuery withQuery : with.getQueries()) {
                String name = withQuery.getName().getValue().toLowerCase(ENGLISH);
                if (withScopeBuilder.containsNamedQuery(name)) {
                    throw semanticException(withQuery, "WITH query name '%s' specified more than once", name);
                }

                boolean isRecursive = false;
                if (with.isRecursive()) {
                    // cannot nest pattern recognition within recursive query
                    preOrder(withQuery.getQuery()).filter(child -> child instanceof PatternRecognitionRelation || child instanceof RowPattern).findFirst().ifPresent(nested -> {
                        throw semanticException(nested, "nested row pattern recognition in recursive query");
                    });
                    isRecursive = tryProcessRecursiveQuery(withQuery, name, withScopeBuilder);
                    // WITH query is not shaped accordingly to the rules for expandable query and will be processed like a plain WITH query.
                    // Since RECURSIVE is specified, any reference to WITH query name is considered a recursive reference and is not allowed.
                    if (!isRecursive) {
                        List<Node> recursiveReferences = findReferences(withQuery.getQuery(), withQuery.getName());
                        if (!recursiveReferences.isEmpty()) {
                            throw semanticException(recursiveReferences.get(0), "recursive reference not allowed in this context");
                        }
                    }
                }

                if (!isRecursive) {
                    Query query = withQuery.getQuery();
                    process(query, withScopeBuilder.build());

                    // check if all or none of the columns are explicitly alias
                    if (withQuery.getColumnNames().isPresent()) {
                        validateColumnAliases(withQuery.getColumnNames().get(), analysis.getOutputDescriptor(query).getVisibleFieldCount());
                    }

                    withScopeBuilder.withNamedQuery(name, withQuery);
                }
            }
            Scope withScope = withScopeBuilder.build();
            analysis.setScope(with, withScope);
            return withScope;
        }

        private void validateColumnAliases(List<Identifier> columnAliases, int sourceColumnSize) {
            validateColumnAliasesCount(columnAliases, sourceColumnSize);
            Set<String> names = new HashSet<>();
            for (Identifier identifier : columnAliases) {
                if (names.contains(identifier.getValue().toLowerCase(ENGLISH))) {
                    throw semanticException(identifier, "Column name '%s' specified more than once", identifier.getValue());
                }
                names.add(identifier.getValue().toLowerCase(ENGLISH));
            }
        }

        private boolean tryProcessRecursiveQuery(WithQuery withQuery, String name, Scope.Builder withScopeBuilder) {
            if (!withQuery.getColumnNames().isPresent()) {
                throw semanticException(withQuery, "missing column aliases in recursive WITH query");
            }
            preOrder(withQuery.getQuery()).filter(child -> child instanceof With && ((With) child).isRecursive()).findFirst().ifPresent(child -> {
                throw semanticException(child, "nested recursive WITH query");
            });
            // if RECURSIVE is specified, all queries in the WITH list are considered potentially recursive
            // try resolve WITH query as expandable query
            // a) validate shape of the query and location of recursive reference
            if (!(withQuery.getQuery().getQueryBody() instanceof Union)) {
                return false;
            }
            Union union = (Union) withQuery.getQuery().getQueryBody();
            if (union.getRelations().size() != 2) {
                return false;
            }
            Relation anchor = union.getRelations().get(0);
            Relation step = union.getRelations().get(1);
            List<Node> anchorReferences = findReferences(anchor, withQuery.getName());
            if (!anchorReferences.isEmpty()) {
                throw semanticException(anchorReferences.get(0), "WITH table name is referenced in the base relation of recursion");
            }
            // a WITH query is linearly recursive if it has a single recursive reference
            List<Node> stepReferences = findReferences(step, withQuery.getName());
            if (stepReferences.size() > 1) {
                throw semanticException(stepReferences.get(1), "multiple recursive references in the step relation of recursion");
            }
            if (stepReferences.size() != 1) {
                return false;
            }
            // search for QuerySpecification in parenthesized subquery
            Relation specification = step;
            while (specification instanceof TableSubquery) {
                Query query = ((TableSubquery) specification).getQuery();
                query.getLimit().ifPresent(limit -> {
                    throw semanticException(limit, "FETCH FIRST / LIMIT clause in the step relation of recursion");
                });
                specification = query.getQueryBody();
            }
            if (!(specification instanceof QuerySpecification) || !(((QuerySpecification) specification).getFrom().isPresent())) {
                throw semanticException(stepReferences.get(0), "recursive reference outside of FROM clause of the step relation of recursion");
            }
            Relation from = ((QuerySpecification) specification).getFrom().get();
            List<Node> fromReferences = findReferences(from, withQuery.getName());
            if (fromReferences.isEmpty()) {
                throw semanticException(stepReferences.get(0), "recursive reference outside of FROM clause of the step relation of recursion");
            }

            // b) validate top-level shape of recursive query
            withQuery.getQuery().getWith().ifPresent(innerWith -> {
                throw semanticException(innerWith, "immediate WITH clause in recursive query");
            });
            withQuery.getQuery().getOrderBy().ifPresent(orderBy -> {
                throw semanticException(orderBy, "immediate ORDER BY clause in recursive query");
            });
            withQuery.getQuery().getOffset().ifPresent(offset -> {
                throw semanticException(offset, "immediate OFFSET clause in recursive query");
            });
            withQuery.getQuery().getLimit().ifPresent(limit -> {
                throw semanticException(limit, "immediate FETCH FIRST / LIMIT clause in recursive query");
            });

            // shape validation complete - process query as expandable query
            Scope parentScope = withScopeBuilder.build();
            // process expandable query -- anchor
            Scope anchorScope = process(anchor, parentScope);
            // set aliases in anchor scope as defined for WITH query. Recursion step will refer to anchor fields by aliases.
            Scope aliasedAnchorScope = setAliases(anchorScope, withQuery.getName(), withQuery.getColumnNames().get());
            // record expandable query base scope for recursion step analysis
            Node recursiveReference = fromReferences.get(0);
            // process expandable query -- recursion step
            Scope stepScope = process(step, parentScope);

            // verify anchor and step have matching descriptors
            RelationType anchorType = aliasedAnchorScope.getRelationType().withOnlyVisibleFields();
            RelationType stepType = stepScope.getRelationType().withOnlyVisibleFields();
            if (anchorType.getVisibleFieldCount() != stepType.getVisibleFieldCount()) {
                throw semanticException(step, "base and step relations of recursion have different number of fields: %s, %s", anchorType.getVisibleFieldCount(), stepType.getVisibleFieldCount());
            }

            analysis.setScope(withQuery.getQuery(), aliasedAnchorScope);
            withScopeBuilder.withNamedQuery(name, withQuery);
            return true;
        }

        private Scope setAliases(Scope scope, Identifier tableName, List<Identifier> columnNames) {
            RelationType oldDescriptor = scope.getRelationType();
            validateColumnAliases(columnNames, oldDescriptor.getVisibleFieldCount());
            RelationType newDescriptor = oldDescriptor.withAlias(tableName.getValue(), columnNames.stream().map(Identifier::getValue).collect(toImmutableList()));

            Streams.forEachPair(
                    oldDescriptor.getAllFields().stream(),
                    newDescriptor.getAllFields().stream(),
                    (newField, field) -> analysis.addSourceColumns(newField, analysis.getSourceColumns(field)));
            return scope.withRelationType(newDescriptor);
        }

        private List<Node> findReferences(Node node, Identifier name) {
            Stream<Node> allReferences = preOrder(node).filter(isTableWithName(name));

            // TODO: recursive references could be supported in subquery before the point of shadowing.
            //currently, the recursive query name is considered shadowed in the whole subquery if the subquery defines a common table with the same name
            Set<Node> shadowedReferences = preOrder(node).filter(isQueryWithNameShadowed(name))
                    .flatMap(query -> preOrder(query).filter(isTableWithName(name))).collect(toImmutableSet());

            return allReferences.filter(reference -> !shadowedReferences.contains(reference)).collect(toImmutableList());
        }

        private Predicate<Node> isTableWithName(Identifier name) {
            return node -> {
                if (!(node instanceof com.github.melin.sqlflow.tree.relation.Table)) {
                    return false;
                }
                com.github.melin.sqlflow.tree.relation.Table table = (Table) node;
                QualifiedName tableName = table.getName();
                return !tableName.getPrefix().isPresent() && tableName.hasSuffix(QualifiedName.of(name.getValue()));
            };
        }

        private Predicate<Node> isQueryWithNameShadowed(Identifier name) {
            return node -> {
                if (!(node instanceof Query)) {
                    return false;
                }
                Query query = (Query) node;
                if (!query.getWith().isPresent()) {
                    return false;
                }
                return query.getWith().get().getQueries().stream()
                        .map(WithQuery::getName).map(Identifier::getValue)
                        .anyMatch(withQueryName -> withQueryName.equalsIgnoreCase(name.getValue()));
            };
        }

        @Override
        public Scope visitAliasedRelation(AliasedRelation relation, Optional<Scope> scope) {
            Scope relationScope = process(relation.getRelation(), scope);

            // todo this check should be inside of TupleDescriptor.withAlias, but the exception needs the node object
            RelationType relationType = relationScope.getRelationType();
            if (relation.getColumnNames() != null) {
                int totalColumns = relationType.getVisibleFieldCount();
                if (totalColumns != relation.getColumnNames().size()) {
                    throw semanticException(relation, "Column alias list has %s entries but '%s' has %s columns available", relation.getColumnNames().size(), relation.getAlias(), totalColumns);
                }
            }

            List<String> aliases = null;
            Collection<Field> inputFields = relationType.getAllFields();
            if (relation.getColumnNames() != null) {
                aliases = relation.getColumnNames().stream().map(Identifier::getValue).collect(Collectors.toList());
                // hidden fields are not exposed when there are column aliases
                inputFields = relationType.getVisibleFields();
            }

            RelationType descriptor = relationType.withAlias(relation.getAlias().getValue(), aliases);

            checkArgument(inputFields.size() == descriptor.getAllFieldCount(), "Expected %s fields, got %s", descriptor.getAllFieldCount(), inputFields.size());

            Streams.forEachPair(descriptor.getAllFields().stream(), inputFields.stream(),
                    (newField, field) -> analysis.addSourceColumns(newField, analysis.getSourceColumns(field)));

            return createAndAssignScope(relation, scope, descriptor);
        }

        @Override
        public Scope visitSampledRelation(SampledRelation relation, Optional<Scope> scope) {
            Expression samplePercentage = relation.getSamplePercentage();

            ExpressionAnalyzer.analyzeExpressions(metadataService, sqlParser, ImmutableList.of(samplePercentage), analysis.getParameters()).getExpressionTypes();

            Scope relationScope = process(relation.getRelation(), scope);
            return createAndAssignScope(relation, scope, relationScope.getRelationType());
        }

        @Override
        public Scope visitSetOperation(SetOperation node, Optional<Scope> scope) {
            checkState(node.getRelations().size() >= 2);

            List<RelationType> childrenTypes = node.getRelations().stream()
                    .map(relation -> process(relation, scope).getRelationType().withOnlyVisibleFields())
                    .collect(toImmutableList());

            Field[] outputFields = childrenTypes.get(0).getVisibleFields().stream()
                    .toArray(Field[]::new);

            Field[] outputDescriptorFields = new Field[outputFields.length];
            RelationType firstDescriptor = childrenTypes.get(0);
            for (int i = 0; i < outputFields.length; i++) {
                Field oldField = firstDescriptor.getFieldByIndex(i);
                outputDescriptorFields[i] = new Field(
                        oldField.getRelationAlias(),
                        oldField.getName(),
                        oldField.getOriginTable(),
                        oldField.getOriginColumnName(),
                        oldField.isAliased());

                int index = i; // Variable used in Lambda should be final
                analysis.addSourceColumns(
                        outputDescriptorFields[index],
                        childrenTypes.stream()
                                .map(relationType -> relationType.getFieldByIndex(index))
                                .flatMap(field -> analysis.getSourceColumns(field).stream())
                                .collect(toImmutableSet()));
            }

            return createAndAssignScope(node, scope, outputDescriptorFields);
        }

        @Override
        public Scope visitJoin(Join node, Optional<Scope> scope) {
            JoinCriteria criteria = node.getCriteria().orElse(null);
            if (criteria instanceof NaturalJoin) {
                throw semanticException(node, "Natural join not supported");
            }

            Scope left = process(node.getLeft(), scope);
            Scope right = process(node.getRight(), isLateralRelation(node.getRight()) ? Optional.of(left) : scope);

            if (isLateralRelation(node.getRight())) {
                if (node.getType() == Join.Type.RIGHT || node.getType() == Join.Type.FULL) {
                    Stream<Expression> leftScopeReferences = ScopeReferenceExtractor.getReferencesToScope(node.getRight(), analysis, left);
                    leftScopeReferences.findFirst().ifPresent(reference -> {
                        throw semanticException(reference, "LATERAL reference not allowed in %s JOIN", node.getType().name());
                    });
                }
                if (isUnnestRelation(node.getRight())) {
                    if (criteria != null) {
                        if (!(criteria instanceof JoinOn) || !((JoinOn) criteria).getExpression().equals(TRUE_LITERAL)) {
                            throw semanticException(
                                    criteria instanceof JoinOn ? ((JoinOn) criteria).getExpression() : node,
                                    "%s JOIN involving UNNEST is only supported with condition ON TRUE",
                                    node.getType().name());
                        }
                    }
                } else if (node.getType() == Join.Type.FULL) {
                    if (!(criteria instanceof JoinOn) || !((JoinOn) criteria).getExpression().equals(TRUE_LITERAL)) {
                        throw semanticException(
                                criteria instanceof JoinOn ? ((JoinOn) criteria).getExpression() : node,
                                "FULL JOIN involving LATERAL relation is only supported with condition ON TRUE");
                    }
                }
            }

            if (criteria instanceof JoinUsing) {
                return analyzeJoinUsing(node, ((JoinUsing) criteria).getColumns(), scope, left, right);
            }

            Scope output = createAndAssignScope(node, scope, left.getRelationType().joinWith(right.getRelationType()));

            if (node.getType() == Join.Type.CROSS || node.getType() == Join.Type.IMPLICIT) {
                return output;
            }
            if (criteria instanceof JoinOn) {
                Expression expression = ((JoinOn) criteria).getExpression();
                Analyzer.verifyNoAggregateWindowOrGroupingFunctions(metadataService, expression, "JOIN clause");

                // Need to register coercions in case when join criteria requires coercion (e.g. join on char(1) = char(2))
                // Correlations are only currently support in the join criteria for INNER joins
                ExpressionAnalyzer.analyzeExpression(output, analysis, metadataService, sqlParser, expression);

                analysis.setJoinCriteria(node, expression);
            } else {
                throw new UnsupportedOperationException("Unsupported join criteria: " + criteria.getClass().getName());
            }

            return output;
        }

        private Scope analyzeJoinUsing(Join node, List<Identifier> columns, Optional<Scope> scope, Scope left, Scope right) {
            List<Field> joinFields = new ArrayList<>();

            List<Field> leftJoinFields = new ArrayList<>();
            List<Field> rightJoinFields = new ArrayList<>();

            Set<Identifier> seen = new HashSet<>();
            for (Identifier column : columns) {
                if (!seen.add(column)) {
                    throw semanticException(column, "Column '%s' appears multiple times in USING clause", column.getValue());
                }

                Optional<ResolvedField> leftField = left.tryResolveField(column);
                Optional<ResolvedField> rightField = right.tryResolveField(column);

                if (!leftField.isPresent()) {
                    throw semanticException(column, "Column '%s' is missing from left side of join", column.getValue());
                }
                if (!rightField.isPresent()) {
                    throw semanticException(column, "Column '%s' is missing from right side of join", column.getValue());
                }

                joinFields.add(Field.newUnqualified(column.getValue()));

                leftJoinFields.add(leftField.get().getField());
                rightJoinFields.add(rightField.get().getField());

            }

            ImmutableList.Builder<Field> outputs = ImmutableList.builder();
            outputs.addAll(joinFields);

            ImmutableList.Builder<Field> leftFields = ImmutableList.builder();
            for (int i = 0; i < left.getRelationType().getAllFieldCount(); i++) {
                if (!leftJoinFields.contains(i)) {
                    outputs.add(left.getRelationType().getFieldByIndex(i));
                    leftFields.add(left.getRelationType().getFieldByIndex(i));
                }
            }

            ImmutableList.Builder<Field> rightFields = ImmutableList.builder();
            for (int i = 0; i < right.getRelationType().getAllFieldCount(); i++) {
                if (!rightJoinFields.contains(i)) {
                    outputs.add(right.getRelationType().getFieldByIndex(i));
                    rightFields.add(left.getRelationType().getFieldByIndex(i));
                }
            }

            analysis.setJoinUsing(node, new Analysis.JoinUsingAnalysis(leftJoinFields, rightJoinFields, leftFields.build(), rightFields.build()));

            return createAndAssignScope(node, scope, new RelationType(outputs.build()));
        }

        private boolean isLateralRelation(Relation node) {
            if (node instanceof AliasedRelation) {
                return isLateralRelation(((AliasedRelation) node).getRelation());
            }
            return node instanceof Unnest || node instanceof Lateral;
        }

        private boolean isUnnestRelation(Relation node) {
            if (node instanceof AliasedRelation) {
                return isUnnestRelation(((AliasedRelation) node).getRelation());
            }
            return node instanceof Unnest;
        }

        @Override
        public Scope visitTableSubquery(TableSubquery node, Optional<Scope> scope) {
            StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadataService, sqlParser);
            Scope queryScope = analyzer.analyze(node.getQuery(), scope);
            return createAndAssignScope(node, scope, queryScope.getRelationType());
        }

        @Override
        public Scope visitQuerySpecification(QuerySpecification node, Optional<Scope> scope) {
            // TODO: extract candidate names from SELECT, WHERE, HAVING, GROUP BY and ORDER BY expressions
            // to pass down to analyzeFrom

            Scope sourceScope = analyzeFrom(node, scope);

            analyzeWindowDefinitions(node, sourceScope);
            resolveFunctionCallAndMeasureWindows(node);

            node.getWhere().ifPresent(where -> analyzeWhere(node, sourceScope, where));

            List<Expression> outputExpressions = analyzeSelect(node, sourceScope);
            Analysis.GroupingSetAnalysis groupByAnalysis = analyzeGroupBy(node, sourceScope, outputExpressions);
            analyzeHaving(node, sourceScope);

            Scope outputScope = computeAndAssignOutputScope(node, scope, sourceScope);

            List<Expression> orderByExpressions = emptyList();
            Optional<Scope> orderByScope = Optional.empty();
            if (node.getOrderBy().isPresent()) {
                OrderBy orderBy = node.getOrderBy().get();
                orderByScope = Optional.of(computeAndAssignOrderByScope(orderBy, sourceScope, outputScope));

                orderByExpressions = analyzeOrderBy(node, orderBy.getSortItems(), orderByScope.get());
            }
            analysis.setOrderByExpressions(node, orderByExpressions);

            List<Expression> sourceExpressions = new ArrayList<>();
            analysis.getSelectExpressions(node).stream()
                    .map(SelectExpression::getExpression)
                    .forEach(sourceExpressions::add);
            node.getHaving().ifPresent(sourceExpressions::add);
            for (WindowDefinition windowDefinition : node.getWindows()) {
                WindowSpecification window = windowDefinition.getWindow();
                sourceExpressions.addAll(window.getPartitionBy());
                getSortItemsFromOrderBy(window.getOrderBy()).stream()
                        .map(SortItem::getSortKey)
                        .forEach(sourceExpressions::add);
                if (window.getFrame().isPresent()) {
                    WindowFrame frame = window.getFrame().get();
                    frame.getStart().getValue()
                            .ifPresent(sourceExpressions::add);
                    frame.getEnd()
                            .flatMap(FrameBound::getValue)
                            .ifPresent(sourceExpressions::add);
                    frame.getMeasures().stream()
                            .map(MeasureDefinition::getExpression)
                            .forEach(sourceExpressions::add);
                    frame.getVariableDefinitions().stream()
                            .map(VariableDefinition::getExpression)
                            .forEach(sourceExpressions::add);
                }
            }

            analyzeGroupingOperations(node, sourceExpressions, orderByExpressions);
            analyzeAggregations(node, sourceScope, orderByScope, groupByAnalysis, sourceExpressions, orderByExpressions);
            //analyzeWindowFunctionsAndMeasures(node, outputExpressions, orderByExpressions);

            if (analysis.isAggregation(node) && node.getOrderBy().isPresent()) {
                ImmutableList.Builder<Expression> aggregates = ImmutableList.<Expression>builder()
                        .addAll(groupByAnalysis.getOriginalExpressions())
                        .addAll(extractAggregateFunctions(orderByExpressions, metadataService))
                        .addAll(extractExpressions(orderByExpressions, GroupingOperation.class));

                analysis.setOrderByAggregates(node.getOrderBy().get(), aggregates.build());
            }

            return outputScope;
        }

        private void analyzeGroupingOperations(QuerySpecification node, List<Expression> outputExpressions, List<Expression> orderByExpressions) {
            List<GroupingOperation> groupingOperations = extractExpressions(Iterables.concat(outputExpressions, orderByExpressions), GroupingOperation.class);
            boolean isGroupingOperationPresent = !groupingOperations.isEmpty();

            if (isGroupingOperationPresent && !node.getGroupBy().isPresent()) {
                throw semanticException(node,
                        "A GROUPING() operation can only be used with a corresponding GROUPING SET/CUBE/ROLLUP/GROUP BY clause");
            }

            analysis.setGroupingOperations(node, groupingOperations);
        }

        private void analyzeAggregations(
                QuerySpecification node,
                Scope sourceScope,
                Optional<Scope> orderByScope,
                Analysis.GroupingSetAnalysis groupByAnalysis,
                List<Expression> outputExpressions,
                List<Expression> orderByExpressions) {
            checkState(orderByExpressions.isEmpty() || orderByScope.isPresent(), "non-empty orderByExpressions list without orderByScope provided");

            List<FunctionCall> aggregates = extractAggregateFunctions(Iterables.concat(outputExpressions, orderByExpressions), metadataService);
            analysis.setAggregates(node, aggregates);
        }

        private void resolveFunctionCallAndMeasureWindows(QuerySpecification querySpecification) {
            ImmutableList.Builder<Expression> expressions = ImmutableList.builder();

            // SELECT expressions and ORDER BY expressions can contain window functions
            for (SelectItem item : querySpecification.getSelect().getSelectItems()) {
                if (item instanceof AllColumns) {
                    ((AllColumns) item).getTarget().ifPresent(expressions::add);
                } else if (item instanceof SingleColumn) {
                    expressions.add(((SingleColumn) item).getExpression());
                }
            }
            for (SortItem sortItem : getSortItemsFromOrderBy(querySpecification.getOrderBy())) {
                expressions.add(sortItem.getSortKey());
            }

            for (FunctionCall windowFunction : extractWindowFunctions(expressions.build())) {
                Analysis.ResolvedWindow resolvedWindow = resolveWindowSpecification(querySpecification, windowFunction.getWindow().get());
                analysis.setWindow(windowFunction, resolvedWindow);
            }

            for (WindowOperation measure : extractWindowMeasures(expressions.build())) {
                Analysis.ResolvedWindow resolvedWindow = resolveWindowSpecification(querySpecification, measure.getWindow());
                analysis.setWindow(measure, resolvedWindow);
            }
        }

        private void analyzeWindowDefinitions(QuerySpecification node, Scope scope) {
            for (WindowDefinition windowDefinition : node.getWindows()) {
                CanonicalizationAware<Identifier> canonicalName = CanonicalizationAware.canonicalizationAwareKey(windowDefinition.getName());

                if (analysis.getWindowDefinition(node, canonicalName) != null) {
                    throw semanticException(windowDefinition, "WINDOW name '%s' specified more than once", windowDefinition.getName());
                }

                Analysis.ResolvedWindow resolvedWindow = resolveWindowSpecification(node, windowDefinition.getWindow());
                analysis.addWindowDefinition(node, canonicalName, resolvedWindow);
            }
        }

        private Analysis.ResolvedWindow resolveWindowSpecification(QuerySpecification querySpecification, Window window) {
            if (window instanceof WindowReference) {
                WindowReference windowReference = (WindowReference) window;
                CanonicalizationAware<Identifier> canonicalName = CanonicalizationAware.canonicalizationAwareKey(windowReference.getName());
                Analysis.ResolvedWindow referencedWindow = analysis.getWindowDefinition(querySpecification, canonicalName);
                if (referencedWindow == null) {
                    throw semanticException(windowReference.getName(), "Cannot resolve WINDOW name " + windowReference.getName());
                }

                return new Analysis.ResolvedWindow(
                        referencedWindow.getPartitionBy(),
                        referencedWindow.getOrderBy(),
                        referencedWindow.getFrame(),
                        !referencedWindow.getPartitionBy().isEmpty(),
                        referencedWindow.getOrderBy().isPresent(),
                        referencedWindow.getFrame().isPresent());
            }

            WindowSpecification windowSpecification = (WindowSpecification) window;

            if (windowSpecification.getExistingWindowName().isPresent()) {
                Identifier referencedName = windowSpecification.getExistingWindowName().get();
                CanonicalizationAware<Identifier> canonicalName = CanonicalizationAware.canonicalizationAwareKey(referencedName);
                Analysis.ResolvedWindow referencedWindow = analysis.getWindowDefinition(querySpecification, canonicalName);
                if (referencedWindow == null) {
                    throw semanticException(referencedName, "Cannot resolve WINDOW name " + referencedName);
                }

                // analyze dependencies between this window specification and referenced window specification
                if (!windowSpecification.getPartitionBy().isEmpty()) {
                    throw semanticException(windowSpecification.getPartitionBy().get(0), "WINDOW specification with named WINDOW reference cannot specify PARTITION BY");
                }
                if (windowSpecification.getOrderBy().isPresent() && referencedWindow.getOrderBy().isPresent()) {
                    throw semanticException(windowSpecification.getOrderBy().get(), "Cannot specify ORDER BY if referenced named WINDOW specifies ORDER BY");
                }
                if (referencedWindow.getFrame().isPresent()) {
                    throw semanticException(windowSpecification.getExistingWindowName().get(), "Cannot reference named WINDOW containing frame specification");
                }

                // resolve window
                Optional<OrderBy> orderBy = windowSpecification.getOrderBy();
                boolean orderByInherited = false;
                if (!orderBy.isPresent() && referencedWindow.getOrderBy().isPresent()) {
                    orderBy = referencedWindow.getOrderBy();
                    orderByInherited = true;
                }

                List<Expression> partitionBy = windowSpecification.getPartitionBy();
                boolean partitionByInherited = false;
                if (!referencedWindow.getPartitionBy().isEmpty()) {
                    partitionBy = referencedWindow.getPartitionBy();
                    partitionByInherited = true;
                }

                Optional<WindowFrame> windowFrame = windowSpecification.getFrame();
                boolean frameInherited = false;
                if (!windowFrame.isPresent() && referencedWindow.getFrame().isPresent()) {
                    windowFrame = referencedWindow.getFrame();
                    frameInherited = true;
                }

                return new Analysis.ResolvedWindow(partitionBy, orderBy, windowFrame, partitionByInherited, orderByInherited, frameInherited);
            }

            return new Analysis.ResolvedWindow(windowSpecification.getPartitionBy(), windowSpecification.getOrderBy(), windowSpecification.getFrame(), false, false, false);
        }

        private void analyzeHaving(QuerySpecification node, Scope scope) {
            if (node.getHaving().isPresent()) {
                Expression predicate = node.getHaving().get();

                List<Expression> windowExpressions = extractWindowExpressions(ImmutableList.of(predicate));
                if (!windowExpressions.isEmpty()) {
                    throw semanticException(windowExpressions.get(0), "HAVING clause cannot contain window functions or row pattern measures");
                }

                ExpressionAnalyzer.analyzeExpression(scope, analysis, metadataService, sqlParser, predicate);

                analysis.setHaving(node, predicate);
            }
        }

        private Analysis.GroupingSetAnalysis analyzeGroupBy(QuerySpecification node, Scope scope, List<Expression> outputExpressions) {
            if (node.getGroupBy().isPresent()) {
                ImmutableList.Builder<Set<FieldId>> cubes = ImmutableList.builder();
                ImmutableList.Builder<List<FieldId>> rollups = ImmutableList.builder();
                ImmutableList.Builder<List<Set<FieldId>>> sets = ImmutableList.builder();
                ImmutableList.Builder<Expression> complexExpressions = ImmutableList.builder();
                ImmutableList.Builder<Expression> groupingExpressions = ImmutableList.builder();

                for (GroupingElement groupingElement : node.getGroupBy().get().getGroupingElements()) {
                    if (groupingElement instanceof SimpleGroupBy) {
                        for (Expression column : groupingElement.getExpressions()) {
                            // simple GROUP BY expressions allow ordinals or arbitrary expressions
                            if (column instanceof LongLiteral) {
                                long ordinal = ((LongLiteral) column).getValue();
                                if (ordinal < 1 || ordinal > outputExpressions.size()) {
                                    throw semanticException(column, "GROUP BY position %s is not in select list", ordinal);
                                }

                                column = outputExpressions.get(toIntExact(ordinal - 1));
                                Analyzer.verifyNoAggregateWindowOrGroupingFunctions(metadataService, column, "GROUP BY clause");
                            } else {
                                Analyzer.verifyNoAggregateWindowOrGroupingFunctions(metadataService, column, "GROUP BY clause");
                                ExpressionAnalyzer.analyzeExpression(scope, analysis, metadataService, sqlParser, column);
                            }

                            ResolvedField field = analysis.getColumnReferenceFields().get(NodeRef.of(column));
                            if (field != null) {
                                sets.add(ImmutableList.of(ImmutableSet.of(field.getFieldId())));
                            } else {
                                complexExpressions.add(column);
                            }

                            groupingExpressions.add(column);
                        }
                    } else {
                        for (Expression column : groupingElement.getExpressions()) {
                            ExpressionAnalyzer.analyzeExpression(scope, analysis, metadataService, sqlParser, column);
                            if (!analysis.getColumnReferences().contains(NodeRef.of(column))) {
                                throw semanticException(column, "GROUP BY expression must be a column reference: %s", column);
                            }

                            groupingExpressions.add(column);
                        }

                        if (groupingElement instanceof Cube) {
                            Set<FieldId> cube = groupingElement.getExpressions().stream()
                                    .map(NodeRef::of)
                                    .map(analysis.getColumnReferenceFields()::get)
                                    .map(ResolvedField::getFieldId)
                                    .collect(toImmutableSet());

                            cubes.add(cube);
                        } else if (groupingElement instanceof Rollup) {
                            List<FieldId> rollup = groupingElement.getExpressions().stream()
                                    .map(NodeRef::of)
                                    .map(analysis.getColumnReferenceFields()::get)
                                    .map(ResolvedField::getFieldId)
                                    .collect(toImmutableList());

                            rollups.add(rollup);
                        } else if (groupingElement instanceof GroupingSets) {
                            List<Set<FieldId>> groupingSets = ((GroupingSets) groupingElement).getSets().stream()
                                    .map(set -> set.stream()
                                            .map(NodeRef::of)
                                            .map(analysis.getColumnReferenceFields()::get)
                                            .map(ResolvedField::getFieldId)
                                            .collect(toImmutableSet()))
                                    .collect(toImmutableList());

                            sets.add(groupingSets);
                        }
                    }
                }

                List<Expression> expressions = groupingExpressions.build();
                for (Expression expression : expressions) {
                    Type type = analysis.getType(expression);
                    if (!type.isComparable()) {
                        throw semanticException(node, "%s is not comparable, and therefore cannot be used in GROUP BY", type);
                    }
                }

                Analysis.GroupingSetAnalysis groupingSets = new Analysis.GroupingSetAnalysis(expressions, cubes.build(), rollups.build(), sets.build(), complexExpressions.build());
                analysis.setGroupingSets(node, groupingSets);

                return groupingSets;
            }

            Analysis.GroupingSetAnalysis result = new Analysis.GroupingSetAnalysis(ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), ImmutableList.of());
            if (hasAggregates(node) || node.getHaving().isPresent()) {
                analysis.setGroupingSets(node, result);
            }

            return result;
        }

        private boolean hasAggregates(QuerySpecification node) {
            List<Node> toExtract = ImmutableList.<Node>builder()
                    .addAll(node.getSelect().getSelectItems())
                    .addAll(getSortItemsFromOrderBy(node.getOrderBy()))
                    .build();

            List<FunctionCall> aggregates = extractAggregateFunctions(toExtract, metadataService);

            return !aggregates.isEmpty();
        }

        private List<Expression> analyzeSelect(QuerySpecification node, Scope scope) {
            ImmutableList.Builder<Expression> outputExpressionBuilder = ImmutableList.builder();
            ImmutableList.Builder<Analysis.SelectExpression> selectExpressionBuilder = ImmutableList.builder();

            for (SelectItem item : node.getSelect().getSelectItems()) {
                if (item instanceof AllColumns) {
                    analyzeSelectAllColumns((AllColumns) item, node, scope, outputExpressionBuilder, selectExpressionBuilder);
                } else if (item instanceof SingleColumn) {
                    analyzeSelectSingleColumn((SingleColumn) item, node, scope, outputExpressionBuilder, selectExpressionBuilder);
                } else {
                    throw new IllegalArgumentException("Unsupported SelectItem type: " + item.getClass().getName());
                }
            }
            analysis.setSelectExpressions(node, selectExpressionBuilder.build());

            return outputExpressionBuilder.build();
        }

        private void validateColumnAliasesCount(List<Identifier> columnAliases, int sourceColumnSize) {
            if (columnAliases.size() != sourceColumnSize) {
                throw semanticException(columnAliases.get(0), "Column alias list has %s entries but relation has %s columns", columnAliases.size(), sourceColumnSize);
            }
        }

        private Scope computeAndAssignOrderByScope(OrderBy node, Scope sourceScope, Scope outputScope) {
            // ORDER BY should "see" both output and FROM fields during initial analysis and non-aggregation query planning
            Scope orderByScope = Scope.builder()
                    .withParent(sourceScope)
                    .withRelationType(outputScope.getRelationId(), outputScope.getRelationType())
                    .build();
            analysis.setScope(node, orderByScope);
            return orderByScope;
        }

        private void analyzeSelectAllColumns(AllColumns allColumns, QuerySpecification node, Scope scope, ImmutableList.Builder<Expression> outputExpressionBuilder, ImmutableList.Builder<SelectExpression> selectExpressionBuilder) {
            // expand * and expression.*
            if (allColumns.getTarget().isPresent()) {
                // analyze AllColumns with target expression (expression.*)
                Expression expression = allColumns.getTarget().get();

                QualifiedName prefix = asQualifiedName(expression);
                if (prefix != null) {
                    // analyze prefix as an 'asterisked identifier chain'
                    Optional<Scope.AsteriskedIdentifierChainBasis> identifierChainBasis = scope.resolveAsteriskedIdentifierChainBasis(prefix, allColumns);
                    if (!identifierChainBasis.isPresent()) {
                        throw semanticException(allColumns, "Unable to resolve reference %s", prefix);
                    }
                    if (identifierChainBasis.get().getBasisType() == TABLE) {
                        RelationType relationType = identifierChainBasis.get().getRelationType().get();
                        List<Field> fields = relationType.resolveVisibleFieldsWithRelationPrefix(Optional.of(prefix));
                        if (fields.isEmpty()) {
                            throw semanticException(allColumns, "SELECT * not allowed from relation that has no columns");
                        }
                        boolean local = scope.isLocalScope(identifierChainBasis.get().getScope().get());
                        analyzeAllColumnsFromTable(fields, allColumns, node, local ? scope : identifierChainBasis.get().getScope().get(), outputExpressionBuilder, selectExpressionBuilder, relationType, local);
                        return;
                    }
                }
                // identifierChainBasis.get().getBasisType == FIELD or target expression isn't a QualifiedName
                analyzeAllFieldsFromRowTypeExpression(expression, allColumns, node, scope, outputExpressionBuilder, selectExpressionBuilder);
            } else {
                // analyze AllColumns without target expression ('*')
                if (!allColumns.getAliases().isEmpty()) {
                    throw semanticException(allColumns, "Column aliases not supported");
                }

                List<Field> fields = (List<Field>) scope.getRelationType().getVisibleFields();
                if (fields.isEmpty()) {
                    if (!node.getFrom().isPresent()) {
                        throw semanticException(allColumns, "SELECT * not allowed in queries without FROM clause");
                    }
                    throw semanticException(allColumns, "SELECT * not allowed from relation that has no columns");
                }

                analyzeAllColumnsFromTable(fields, allColumns, node, scope, outputExpressionBuilder, selectExpressionBuilder, scope.getRelationType(), true);
            }
        }

        private List<Field> filterInaccessibleFields(List<Field> fields) {
            List<Field> accessibleFields = new ArrayList<>();

            //collect fields by table
            ListMultimap<QualifiedObjectName, Field> tableFieldsMap = ArrayListMultimap.create();
            fields.forEach(field -> {
                Optional<QualifiedObjectName> originTable = field.getOriginTable();
                if (originTable.isPresent()) {
                    tableFieldsMap.put(originTable.get(), field);
                } else {
                    // keep anonymous fields accessible
                    accessibleFields.add(field);
                }
            });

            return fields.stream().filter(field -> accessibleFields.contains(field)).collect(toImmutableList());
        }

        private void analyzeAllColumnsFromTable(List<Field> fields, AllColumns allColumns, QuerySpecification node, Scope scope, ImmutableList.Builder<Expression> outputExpressionBuilder, ImmutableList.Builder<SelectExpression> selectExpressionBuilder, RelationType relationType, boolean local) {
            if (!allColumns.getAliases().isEmpty()) {
                validateColumnAliasesCount(allColumns.getAliases(), fields.size());
            }

            ImmutableList.Builder<Field> itemOutputFieldBuilder = ImmutableList.builder();

            for (int i = 0; i < fields.size(); i++) {
                Field field = fields.get(i);
                Expression fieldExpression;
                if (local) {
                    fieldExpression = new FieldReference(relationType.indexOf(field));
                } else {
                    if (!field.getName().isPresent()) {
                        throw semanticException(node.getSelect(), "SELECT * from outer scope table not supported with anonymous columns");
                    }
                    checkState(field.getRelationAlias().isPresent(), "missing relation alias");
                    fieldExpression = new DereferenceExpression(DereferenceExpression.from(field.getRelationAlias().get()), new Identifier(field.getName().get()));
                }
                ExpressionAnalyzer.analyzeExpression(scope, analysis, metadataService, sqlParser, fieldExpression);
                outputExpressionBuilder.add(fieldExpression);
                selectExpressionBuilder.add(new SelectExpression(fieldExpression, Optional.empty()));

                Optional<String> alias = field.getName();
                if (!allColumns.getAliases().isEmpty()) {
                    alias = Optional.of((allColumns.getAliases().get(i)).getValue());
                }

                Field newField = new Field(field.getRelationAlias(), alias, field.getOriginTable(),
                        field.getOriginColumnName(), !allColumns.getAliases().isEmpty() || field.isAliased());
                itemOutputFieldBuilder.add(newField);
                analysis.addSourceColumns(newField, analysis.getSourceColumns(field));
            }
            analysis.setSelectAllResultFields(allColumns, itemOutputFieldBuilder.build());
        }

        private void analyzeAllFieldsFromRowTypeExpression(Expression expression, AllColumns allColumns, QuerySpecification node, Scope scope, ImmutableList.Builder<Expression> outputExpressionBuilder, ImmutableList.Builder<SelectExpression> selectExpressionBuilder) {
            ImmutableList.Builder<Field> itemOutputFieldBuilder = ImmutableList.builder();

            ExpressionAnalysis expressionAnalysis = ExpressionAnalyzer.analyzeExpression(scope, analysis, metadataService, sqlParser, expression);
            Type type = expressionAnalysis.getType(expression);
            if (!(type instanceof RowType)) {
                throw semanticException(node.getSelect(), "expected expression of type Row");
            }
            int referencedFieldsCount = ((RowType) type).getFields().size();
            if (!allColumns.getAliases().isEmpty()) {
                validateColumnAliasesCount(allColumns.getAliases(), referencedFieldsCount);
            }
            //analysis.recordSubqueries(node, expressionAnalysis);

            ImmutableList.Builder<Expression> unfoldedExpressionsBuilder = ImmutableList.builder();
            for (int i = 0; i < referencedFieldsCount; i++) {
                Expression outputExpression = new SubscriptExpression(expression, new LongLiteral("" + (i + 1)));
                outputExpressionBuilder.add(outputExpression);
                ExpressionAnalyzer.analyzeExpression(scope, analysis, metadataService, sqlParser, outputExpression);
                unfoldedExpressionsBuilder.add(outputExpression);

                Optional<String> name = ((RowType) type).getFields().get(i).getName();
                if (!allColumns.getAliases().isEmpty()) {
                    name = Optional.of((allColumns.getAliases().get(i)).getValue());
                }
                itemOutputFieldBuilder.add(Field.newUnqualified(name));
            }
            selectExpressionBuilder.add(new SelectExpression(expression, Optional.of(unfoldedExpressionsBuilder.build())));
            analysis.setSelectAllResultFields(allColumns, itemOutputFieldBuilder.build());
        }

        private void analyzeSelectSingleColumn(SingleColumn singleColumn, QuerySpecification node, Scope scope, ImmutableList.Builder<Expression> outputExpressionBuilder, ImmutableList.Builder<SelectExpression> selectExpressionBuilder) {
            Expression expression = singleColumn.getExpression();
            ExpressionAnalysis expressionAnalysis = ExpressionAnalyzer.analyzeExpression(scope, analysis, metadataService, sqlParser, expression);
            //analysis.recordSubqueries(node, expressionAnalysis);
            outputExpressionBuilder.add(expression);
            selectExpressionBuilder.add(new SelectExpression(expression, Optional.empty()));
        }

        private Scope computeAndAssignOutputScope(QuerySpecification node, Optional<Scope> scope, Scope sourceScope) {
            ImmutableList.Builder<Field> outputFields = ImmutableList.builder();

            for (SelectItem item : node.getSelect().getSelectItems()) {
                if (item instanceof AllColumns) {
                    AllColumns allColumns = (AllColumns) item;

                    List<Field> fields = analysis.getSelectAllResultFields(allColumns);
                    checkNotNull(fields, "output fields is null for select item %s", item);
                    for (int i = 0; i < fields.size(); i++) {
                        Field field = fields.get(i);

                        Optional<String> name;
                        if (!allColumns.getAliases().isEmpty()) {
                            name = Optional.of((allColumns.getAliases().get(i)).getCanonicalValue());
                        } else {
                            name = field.getName();
                        }

                        Field newField = Field.newUnqualified(name, field.getOriginTable(), field.getOriginColumnName(), false);
                        analysis.addSourceColumns(newField, analysis.getSourceColumns(field));
                        outputFields.add(newField);
                    }
                } else if (item instanceof SingleColumn) {
                    SingleColumn column = (SingleColumn) item;

                    Expression expression = column.getExpression();
                    Optional<Identifier> field = column.getAlias();

                    Optional<QualifiedObjectName> originTable = Optional.empty();
                    Optional<String> originColumn = Optional.empty();
                    QualifiedName name = null;

                    if (expression instanceof Identifier) {
                        name = QualifiedName.of(((Identifier) expression).getValue());
                    } else if (expression instanceof DereferenceExpression) {
                        name = DereferenceExpression.getQualifiedName((DereferenceExpression) expression);
                    }

                    if (name != null) {
                        List<Field> matchingFields = sourceScope.getRelationType().resolveFields(name);
                        if (!matchingFields.isEmpty()) {
                            originTable = matchingFields.get(0).getOriginTable();
                            originColumn = matchingFields.get(0).getOriginColumnName();
                        }
                    }

                    if (!field.isPresent()) {
                        if (name != null) {
                            field = Optional.of(getLast(name.getOriginalParts()));
                        }
                    }

                    Field newField = Field.newUnqualified(field.map(Identifier::getValue), originTable, originColumn, column.getAlias().isPresent()); // TODO don't use analysis as a side-channel. Use outputExpressions to look up the type
                    if (originTable.isPresent()) {
                        analysis.addSourceColumns(newField, ImmutableSet.of(new SourceColumn(originTable.get(), originColumn.get())));
                    } else {
                        analysis.addSourceColumns(newField, analysis.getExpressionSourceColumns(expression));
                    }
                    outputFields.add(newField);
                } else {
                    throw new IllegalArgumentException("Unsupported SelectItem type: " + item.getClass().getName());
                }
            }

            return createAndAssignScope(node, scope, outputFields.build());
        }

        private void analyzeWhere(Node node, Scope scope, Expression predicate) {
            Analyzer.verifyNoAggregateWindowOrGroupingFunctions(metadataService, predicate, "WHERE clause");

            ExpressionAnalysis expressionAnalysis = ExpressionAnalyzer.analyzeExpression(scope, analysis, metadataService, sqlParser, predicate);
            expressionAnalysis.getColumnReferences().values().forEach(resolvedField -> {
                analysis.setWhere(resolvedField.getField(), predicate);
            });
        }

        private Scope analyzeFrom(QuerySpecification node, Optional<Scope> scope) {
            if (node.getFrom().isPresent()) {
                return process(node.getFrom().get(), scope);
            }

            Scope result = createScope(scope);
            return result;
        }

        private List<Expression> descriptorToFields(Scope scope) {
            ImmutableList.Builder<Expression> builder = ImmutableList.builder();
            for (int fieldIndex = 0; fieldIndex < scope.getRelationType().getAllFieldCount(); fieldIndex++) {
                FieldReference expression = new FieldReference(fieldIndex);
                builder.add(expression);
                ExpressionAnalyzer.analyzeExpression(scope, analysis, metadataService, sqlParser, expression);
            }
            return builder.build();
        }

        private Scope createAndAssignScope(Node node, Optional<Scope> parentScope) {
            return createAndAssignScope(node, parentScope, emptyList());
        }

        private Scope createAndAssignScope(Node node, Optional<Scope> parentScope, Field... fields) {
            return createAndAssignScope(node, parentScope, new RelationType(fields));
        }

        private Scope createAndAssignScope(Node node, Optional<Scope> parentScope, List<Field> fields) {
            return createAndAssignScope(node, parentScope, new RelationType(fields));
        }

        private Scope createAndAssignScope(Node node, Optional<Scope> parentScope, RelationType relationType) {
            Scope scope = scopeBuilder(parentScope).withRelationType(RelationId.of(node), relationType).build();

            analysis.setScope(node, scope);
            return scope;
        }

        private Scope createScope(Optional<Scope> parentScope) {
            return scopeBuilder(parentScope).build();
        }

        private Scope.Builder scopeBuilder(Optional<Scope> parentScope) {
            Scope.Builder scopeBuilder = Scope.builder();

            if (parentScope.isPresent()) {
                // parent scope represents local query scope hierarchy. Local query scope
                // hierarchy should have outer query scope as ancestor already.
                scopeBuilder.withParent(parentScope.get());
            } else {
                outerQueryScope.ifPresent(scopeBuilder::withOuterQueryParent);
            }

            return scopeBuilder;
        }

        private OutputColumn createOutputColumn(Field field) {
            return new OutputColumn(field.getName().get(), analysis.getSourceColumns(field));
        }
    }

    private static boolean hasScopeAsLocalParent(Scope root, Scope parent) {
        Scope scope = root;
        while (scope.getLocalParent().isPresent()) {
            scope = scope.getLocalParent().get();
            if (scope.equals(parent)) {
                return true;
            }
        }

        return false;
    }
}
