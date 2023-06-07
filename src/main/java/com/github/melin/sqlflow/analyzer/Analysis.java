package com.github.melin.sqlflow.analyzer;

import com.github.melin.sqlflow.metadata.QualifiedObjectName;
import com.github.melin.sqlflow.tree.*;
import com.github.melin.sqlflow.tree.expression.*;
import com.github.melin.sqlflow.tree.join.Join;
import com.github.melin.sqlflow.tree.relation.QuerySpecification;
import com.github.melin.sqlflow.tree.relation.Table;
import com.github.melin.sqlflow.tree.statement.Query;
import com.github.melin.sqlflow.tree.statement.Statement;
import com.github.melin.sqlflow.tree.window.WindowFrame;
import com.github.melin.sqlflow.type.Type;
import com.google.common.collect.*;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.util.*;

import static com.github.melin.sqlflow.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.Boolean.FALSE;
import static java.lang.String.format;
import static java.util.Collections.*;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/24 1:49 PM
 */
public class Analysis {

    private final Statement root;

    private String updateType;

    private Optional<UpdateTarget> target = Optional.empty();

    private final Map<NodeRef<Parameter>, Expression> parameters;

    private final Map<NodeRef<com.github.melin.sqlflow.tree.relation.Table>, Query> namedQueries = new LinkedHashMap<>();

    private final Map<NodeRef<Node>, Scope> scopes = new LinkedHashMap<>();

    private final Map<NodeRef<Expression>, Type> types = new LinkedHashMap<>();

    private final Map<NodeRef<Node>, List<SelectExpression>> selectExpressions = new LinkedHashMap<>();

    private final Map<NodeRef<AllColumns>, List<Field>> selectAllResultFields = new LinkedHashMap<>();

    private final Multimap<Field, SourceColumn> originColumnDetails = ArrayListMultimap.create();

    private final Multimap<SourceColumn, NodeLocation> originFields = ArrayListMultimap.create();

    private final Multimap<QualifiedObjectName, NodeLocation> originTables = ArrayListMultimap.create();

    private final Multimap<NodeRef<Expression>, Field> fieldLineage = ArrayListMultimap.create();

    // Store resolved window specifications for window functions and row pattern measures
    private final Map<NodeRef<Node>, ResolvedWindow> windows = new LinkedHashMap<>();

    // map inner recursive reference in the expandable query to the recursion base scope
    private final Map<NodeRef<Node>, Scope> expandableBaseScopes = new LinkedHashMap<>();

    private final Map<NodeRef<Expression>, ResolvedField> columnReferences = new LinkedHashMap<>();

    private final Map<NodeRef<Join>, Expression> joins = new LinkedHashMap<>();
    private final Map<NodeRef<Join>, JoinUsingAnalysis> joinUsing = new LinkedHashMap<>();
    private final Multimap<Field, Expression> where = ArrayListMultimap.create();
    private final Map<NodeRef<QuerySpecification>, Expression> having = new LinkedHashMap<>();
    private final Map<NodeRef<Node>, List<Expression>> orderByExpressions = new LinkedHashMap<>();
    private final Map<NodeRef<QuerySpecification>, GroupingSetAnalysis> groupingSets = new LinkedHashMap<>();

    // Store resolved window specifications defined in WINDOW clause
    private final Map<NodeRef<QuerySpecification>, Map<CanonicalizationAware<Identifier>, ResolvedWindow>> windowDefinitions = new LinkedHashMap<>();

    private final Map<NodeRef<QuerySpecification>, List<GroupingOperation>> groupingOperations = new LinkedHashMap<>();

    private final Map<NodeRef<QuerySpecification>, List<FunctionCall>> aggregates = new LinkedHashMap<>();

    private final Map<NodeRef<OrderBy>, List<Expression>> orderByAggregates = new LinkedHashMap<>();

    // for recursive view detection
    private final Deque<com.github.melin.sqlflow.tree.relation.Table> tablesForView = new ArrayDeque<>();

    public Analysis(@Nullable Statement root, Map<NodeRef<Parameter>, Expression> parameters) {
        this.root = root;
        this.parameters = ImmutableMap.copyOf(requireNonNull(parameters, "parameters is null"));
    }

    public Statement getStatement() {
        return root;
    }

    public String getUpdateType() {
        return updateType;
    }

    public Optional<Output> getTarget() {
        return target.map(target -> {
            QualifiedObjectName name = target.getName();
            return new Output(name.getCatalogName(), name.getSchemaName(), name.getObjectName(), target.getColumns());
        });
    }

    public void setUpdateType(String updateType) {
        this.updateType = updateType;
    }

    public void setUpdateTarget(QualifiedObjectName targetName, Optional<com.github.melin.sqlflow.tree.relation.Table> targetTable, Optional<List<OutputColumn>> targetColumns) {
        this.target = Optional.of(new UpdateTarget(targetName, targetTable, targetColumns));
    }

    public boolean isUpdateTarget(com.github.melin.sqlflow.tree.relation.Table table) {
        requireNonNull(table, "table is null");
        return target
                .flatMap(UpdateTarget::getTable)
                .map(tableReference -> tableReference == table) // intentional comparison by reference
                .orElse(FALSE);
    }

    public Query getNamedQuery(com.github.melin.sqlflow.tree.relation.Table table) {
        return namedQueries.get(NodeRef.of(table));
    }

    public void registerNamedQuery(com.github.melin.sqlflow.tree.relation.Table tableReference, Query query) {
        requireNonNull(tableReference, "tableReference is null");
        requireNonNull(query, "query is null");

        namedQueries.put(NodeRef.of(tableReference), query);
    }

    public Map<NodeRef<Parameter>, Expression> getParameters() {
        return parameters;
    }

    public void setScope(Node node, Scope scope) {
        scopes.put(NodeRef.of(node), scope);
    }

    public void addTypes(Map<NodeRef<Expression>, Type> types) {
        this.types.putAll(types);
    }

    public Map<NodeRef<Expression>, Type> getTypes() {
        return unmodifiableMap(types);
    }

    public Type getType(Expression expression) {
        Type type = types.get(NodeRef.of(expression));
        if (type == null) {
            return VARCHAR;
        }
        //checkArgument(type != null, "Expression not analyzed: %s", expression);
        return type;
    }

    public void setSelectExpressions(Node node, List<SelectExpression> expressions) {
        selectExpressions.put(NodeRef.of(node), ImmutableList.copyOf(expressions));
    }

    public List<SelectExpression> getSelectExpressions(Node node) {
        return selectExpressions.get(NodeRef.of(node));
    }

    public void setSelectAllResultFields(AllColumns node, List<Field> expressions) {
        selectAllResultFields.put(NodeRef.of(node), ImmutableList.copyOf(expressions));
    }

    public List<Field> getSelectAllResultFields(AllColumns node) {
        return selectAllResultFields.get(NodeRef.of(node));
    }

    public void addSourceColumns(Field field, Set<SourceColumn> sourceColumn) {
        originColumnDetails.putAll(field, sourceColumn);
    }

    public Set<SourceColumn> getSourceColumns(Field field) {
        return ImmutableSet.copyOf(originColumnDetails.get(field));
    }

    public void addOriginField(SourceColumn sourceColumn, NodeLocation location) {
        originFields.put(sourceColumn, location);
    }

    public Set<NodeLocation> getOriginField(SourceColumn sourceColumn) {
        return ImmutableSet.copyOf(originFields.get(sourceColumn));
    }

    public void addOriginTable(QualifiedObjectName name, NodeLocation location) {
        originTables.put(name, location);
    }

    public Set<NodeLocation> getOriginTable(QualifiedObjectName name) {
        return ImmutableSet.copyOf(originTables.get(name));
    }
    
    public void addExpressionFields(Expression expression, Collection<Field> fields) {
        fieldLineage.putAll(NodeRef.of(expression), fields);
    }

    public Set<SourceColumn> getExpressionSourceColumns(Expression expression) {
        return fieldLineage.get(NodeRef.of(expression)).stream()
                .flatMap(field -> getSourceColumns(field).stream())
                .collect(toImmutableSet());
    }

    public void setWindow(Node node, ResolvedWindow window) {
        windows.put(NodeRef.of(node), window);
    }

    public ResolvedWindow getWindow(Node node) {
        return windows.get(NodeRef.of(node));
    }

    public Expression getHaving(QuerySpecification query) {
        return having.get(NodeRef.of(query));
    }

    public void setHaving(QuerySpecification node, Expression expression) {
        having.put(NodeRef.of(node), expression);
    }

    public void addWindowDefinition(QuerySpecification query, CanonicalizationAware<Identifier> name, ResolvedWindow window) {
        windowDefinitions.computeIfAbsent(NodeRef.of(query), key -> new LinkedHashMap<>())
                .put(name, window);
    }

    public ResolvedWindow getWindowDefinition(QuerySpecification query, CanonicalizationAware<Identifier> name) {
        Map<CanonicalizationAware<Identifier>, ResolvedWindow> windows = windowDefinitions.get(NodeRef.of(query));
        if (windows != null) {
            return windows.get(name);
        }

        return null;
    }

    public void setGroupingOperations(QuerySpecification querySpecification, List<GroupingOperation> groupingOperations) {
        this.groupingOperations.put(NodeRef.of(querySpecification), ImmutableList.copyOf(groupingOperations));
    }

    public List<GroupingOperation> getGroupingOperations(QuerySpecification querySpecification) {
        return Optional.ofNullable(groupingOperations.get(NodeRef.of(querySpecification)))
                .orElse(emptyList());
    }

    public void setAggregates(QuerySpecification node, List<FunctionCall> aggregates) {
        this.aggregates.put(NodeRef.of(node), ImmutableList.copyOf(aggregates));
    }

    public List<FunctionCall> getAggregates(QuerySpecification query) {
        return aggregates.get(NodeRef.of(query));
    }

    public void setOrderByAggregates(OrderBy node, List<Expression> aggregates) {
        this.orderByAggregates.put(NodeRef.of(node), ImmutableList.copyOf(aggregates));
    }

    public List<Expression> getOrderByAggregates(OrderBy node) {
        return orderByAggregates.get(NodeRef.of(node));
    }

    public void setExpandableBaseScope(Node node, Scope scope) {
        expandableBaseScopes.put(NodeRef.of(node), scope);
    }

    public Optional<Scope> getExpandableBaseScope(Node node) {
        return Optional.ofNullable(expandableBaseScopes.get(NodeRef.of(node)));
    }

    public Set<NodeRef<Expression>> getColumnReferences() {
        return unmodifiableSet(columnReferences.keySet());
    }

    public Map<NodeRef<Expression>, ResolvedField> getColumnReferenceFields() {
        return unmodifiableMap(columnReferences);
    }

    public ResolvedField getResolvedField(Expression expression) {
        checkArgument(isColumnReference(expression), "Expression is not a column reference: %s", expression);
        return columnReferences.get(NodeRef.of(expression));
    }

    public boolean isColumnReference(Expression expression) {
        requireNonNull(expression, "expression is null");
        return columnReferences.containsKey(NodeRef.of(expression));
    }

    public void addColumnReferences(Map<NodeRef<Expression>, ResolvedField> columnReferences) {
        this.columnReferences.putAll(columnReferences);
    }

    public void registerTableForView(com.github.melin.sqlflow.tree.relation.Table tableReference) {
        tablesForView.push(requireNonNull(tableReference, "tableReference is null"));
    }

    public void unregisterTableForView() {
        tablesForView.pop();
    }

    public void setJoinCriteria(Join node, Expression criteria) {
        joins.put(NodeRef.of(node), criteria);
    }

    public Expression getJoinCriteria(Join join) {
        return joins.get(NodeRef.of(join));
    }

    public void setJoinUsing(Join node, JoinUsingAnalysis analysis) {
        joinUsing.put(NodeRef.of(node), analysis);
    }

    public JoinUsingAnalysis getJoinUsing(Join node) {
        return joinUsing.get(NodeRef.of(node));
    }

    public void setWhere(Field field, Expression expression) {
        where.put(field, expression);
    }

    public Multimap<Field, Expression> getWhere() {
        return where;
    }

    public void setOrderByExpressions(Node node, List<Expression> items) {
        orderByExpressions.put(NodeRef.of(node), ImmutableList.copyOf(items));
    }

    public List<Expression> getOrderByExpressions(Node node) {
        return orderByExpressions.get(NodeRef.of(node));
    }

    public void setGroupingSets(QuerySpecification node, GroupingSetAnalysis groupingSets) {
        this.groupingSets.put(NodeRef.of(node), groupingSets);
    }

    public boolean isAggregation(QuerySpecification node) {
        return groupingSets.containsKey(NodeRef.of(node));
    }

    public GroupingSetAnalysis getGroupingSets(QuerySpecification node) {
        return groupingSets.get(NodeRef.of(node));
    }

    public boolean hasTableInView(com.github.melin.sqlflow.tree.relation.Table tableReference) {
        return tablesForView.contains(tableReference);
    }

    public RelationType getOutputDescriptor(Node node) {
        return getScope(node).getRelationType();
    }

    public Scope getScope(Node node) {
        return tryGetScope(node).orElseThrow(() -> new IllegalArgumentException(format("Analysis does not contain information for node: %s", node)));
    }

    public Optional<Scope> tryGetScope(Node node) {
        NodeRef<Node> key = NodeRef.of(node);
        if (scopes.containsKey(key)) {
            return Optional.of(scopes.get(key));
        }

        return Optional.empty();
    }

    @Immutable
    public static final class SelectExpression {
        // expression refers to a select item, either to be returned directly, or unfolded by all-fields reference
        // unfoldedExpressions applies to the latter case, and is a list of subscript expressions
        // referencing each field of the row.
        private final Expression expression;
        private final Optional<List<Expression>> unfoldedExpressions;

        public SelectExpression(Expression expression, Optional<List<Expression>> unfoldedExpressions) {
            this.expression = requireNonNull(expression, "expression is null");
            this.unfoldedExpressions = requireNonNull(unfoldedExpressions);
        }

        public Expression getExpression() {
            return expression;
        }

        public Optional<List<Expression>> getUnfoldedExpressions() {
            return unfoldedExpressions;
        }
    }

    public static class LineageLocation {
        private final NodeLocation location;
        private final String name;

        public LineageLocation(NodeLocation location, String name) {
            this.location = location;
            this.name = name;
        }

        public NodeLocation getLocation() {
            return location;
        }

        public String getName() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LineageLocation that = (LineageLocation) o;
            return Objects.equals(location, that.location) && Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(location, name);
        }
    }

    public static class SourceColumn {
        private final QualifiedObjectName tableName;
        private final String columnName;

        public SourceColumn(QualifiedObjectName tableName, String columnName) {
            this.tableName = requireNonNull(tableName, "tableName is null");
            this.columnName = requireNonNull(columnName, "columnName is null");
        }

        public QualifiedObjectName getTableName() {
            return tableName;
        }

        public String getColumnName() {
            return columnName;
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableName, columnName);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if ((obj == null) || (getClass() != obj.getClass())) {
                return false;
            }
            SourceColumn entry = (SourceColumn) obj;
            return Objects.equals(tableName, entry.tableName) &&
                    Objects.equals(columnName, entry.columnName);
        }
    }

    private static class UpdateTarget {
        private final QualifiedObjectName name;
        private final Optional<com.github.melin.sqlflow.tree.relation.Table> table;
        private final Optional<List<OutputColumn>> columns;

        public UpdateTarget(QualifiedObjectName name, Optional<com.github.melin.sqlflow.tree.relation.Table> table, Optional<List<OutputColumn>> columns) {
            this.name = requireNonNull(name, "name is null");
            this.table = requireNonNull(table, "table is null");
            this.columns = requireNonNull(columns, "columns is null").map(ImmutableList::copyOf);
        }

        public QualifiedObjectName getName() {
            return name;
        }

        public Optional<Table> getTable() {
            return table;
        }

        public Optional<List<OutputColumn>> getColumns() {
            return columns;
        }
    }

    public static class ResolvedWindow {
        private final List<Expression> partitionBy;
        private final Optional<OrderBy> orderBy;
        private final Optional<WindowFrame> frame;
        private final boolean partitionByInherited;
        private final boolean orderByInherited;
        private final boolean frameInherited;

        public ResolvedWindow(List<Expression> partitionBy, Optional<OrderBy> orderBy, Optional<WindowFrame> frame, boolean partitionByInherited, boolean orderByInherited, boolean frameInherited) {
            this.partitionBy = requireNonNull(partitionBy, "partitionBy is null");
            this.orderBy = requireNonNull(orderBy, "orderBy is null");
            this.frame = requireNonNull(frame, "frame is null");
            this.partitionByInherited = partitionByInherited;
            this.orderByInherited = orderByInherited;
            this.frameInherited = frameInherited;
        }

        public List<Expression> getPartitionBy() {
            return partitionBy;
        }

        public Optional<OrderBy> getOrderBy() {
            return orderBy;
        }

        public Optional<WindowFrame> getFrame() {
            return frame;
        }

        public boolean isPartitionByInherited() {
            return partitionByInherited;
        }

        public boolean isOrderByInherited() {
            return orderByInherited;
        }

        public boolean isFrameInherited() {
            return frameInherited;
        }
    }

    public static class GroupingSetAnalysis {
        private final List<Expression> originalExpressions;

        private final List<Set<FieldId>> cubes;
        private final List<List<FieldId>> rollups;
        private final List<List<Set<FieldId>>> ordinarySets;
        private final List<Expression> complexExpressions;

        public GroupingSetAnalysis(
                List<Expression> originalExpressions,
                List<Set<FieldId>> cubes,
                List<List<FieldId>> rollups,
                List<List<Set<FieldId>>> ordinarySets,
                List<Expression> complexExpressions) {
            this.originalExpressions = ImmutableList.copyOf(originalExpressions);
            this.cubes = ImmutableList.copyOf(cubes);
            this.rollups = ImmutableList.copyOf(rollups);
            this.ordinarySets = ImmutableList.copyOf(ordinarySets);
            this.complexExpressions = ImmutableList.copyOf(complexExpressions);
        }

        public List<Expression> getOriginalExpressions() {
            return originalExpressions;
        }

        public List<Set<FieldId>> getCubes() {
            return cubes;
        }

        public List<List<FieldId>> getRollups() {
            return rollups;
        }

        public List<List<Set<FieldId>>> getOrdinarySets() {
            return ordinarySets;
        }

        public List<Expression> getComplexExpressions() {
            return complexExpressions;
        }

        public Set<FieldId> getAllFields() {
            return Streams.concat(
                            cubes.stream().flatMap(Collection::stream),
                            rollups.stream().flatMap(Collection::stream),
                            ordinarySets.stream()
                                    .flatMap(Collection::stream)
                                    .flatMap(Collection::stream))
                    .collect(toImmutableSet());
        }
    }

    public static final class JoinUsingAnalysis {
        private final List<Field> leftJoinFields;
        private final List<Field> rightJoinFields;
        private final List<Field> otherLeftFields;
        private final List<Field> otherRightFields;

        JoinUsingAnalysis(List<Field> leftJoinFields, List<Field> rightJoinFields, List<Field> otherLeftFields, List<Field> otherRightFields) {
            this.leftJoinFields = ImmutableList.copyOf(leftJoinFields);
            this.rightJoinFields = ImmutableList.copyOf(rightJoinFields);
            this.otherLeftFields = ImmutableList.copyOf(otherLeftFields);
            this.otherRightFields = ImmutableList.copyOf(otherRightFields);

            checkArgument(leftJoinFields.size() == rightJoinFields.size(), "Expected join fields for left and right to have the same size");
        }

        public List<Field> getLeftJoinFields() {
            return leftJoinFields;
        }

        public List<Field> getRightJoinFields() {
            return rightJoinFields;
        }

        public List<Field> getOtherLeftFields() {
            return otherLeftFields;
        }

        public List<Field> getOtherRightFields() {
            return otherRightFields;
        }
    }

    public static class Range {
        private final Optional<Integer> atLeast;
        private final Optional<Integer> atMost;

        public Range(Optional<Integer> atLeast, Optional<Integer> atMost) {
            this.atLeast = requireNonNull(atLeast, "atLeast is null");
            this.atMost = requireNonNull(atMost, "atMost is null");
        }

        public Optional<Integer> getAtLeast() {
            return atLeast;
        }

        public Optional<Integer> getAtMost() {
            return atMost;
        }
    }
}
