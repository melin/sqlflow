package com.github.melin.sqlflow;

import com.github.melin.sqlflow.tree.*;
import com.github.melin.sqlflow.tree.expression.*;
import com.github.melin.sqlflow.tree.merge.*;
import com.github.melin.sqlflow.tree.relation.*;
import com.github.melin.sqlflow.tree.statement.*;
import com.github.melin.sqlflow.tree.type.*;
import com.github.melin.sqlflow.tree.filter.FetchFirst;
import com.github.melin.sqlflow.tree.filter.Limit;
import com.github.melin.sqlflow.tree.filter.Offset;
import com.github.melin.sqlflow.tree.group.*;
import com.github.melin.sqlflow.tree.group.*;
import com.github.melin.sqlflow.tree.join.Join;
import com.github.melin.sqlflow.tree.literal.*;
import com.github.melin.sqlflow.tree.window.*;
import com.github.melin.sqlflow.tree.window.rowPattern.*;
import com.github.melin.sqlflow.tree.*;
import com.github.melin.sqlflow.tree.expression.*;
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

import javax.annotation.Nullable;

/**
 * huaixin 2021/12/18 9:53 PM
 */
public abstract class AstVisitor<R, C> {
    public R process(Node node) {
        return process(node, null);
    }

    public R process(Node node, @Nullable C context) {
        return node.accept(this, context);
    }

    public R visitNode(Node node, C context) {
        return null;
    }

    public R visitExpression(Expression node, C context) {
        return visitNode(node, context);
    }

    public R visitDereferenceExpression(DereferenceExpression node, C context) {
        return visitExpression(node, context);
    }

    public R visitStatement(Statement node, C context) {
        return visitNode(node, context);
    }

    public R visitCreateTableAsSelect(CreateTableAsSelect node, C context) {
        return visitStatement(node, context);
    }

    public R visitCreateView(CreateView node, C context) {
        return visitStatement(node, context);
    }

    public R visitCreateMaterializedView(CreateMaterializedView node, C context) {
        return visitStatement(node, context);
    }

    public R visitDelete(Delete node, C context)
    {
        return visitStatement(node, context);
    }

    public R visitUpdate(Update node, C context)
    {
        return visitStatement(node, context);
    }

    public R visitUpdateAssignment(UpdateAssignment node, C context)
    {
        return visitNode(node, context);
    }

    public R visitMerge(Merge node, C context)
    {
        return visitStatement(node, context);
    }

    public R visitMergeCase(MergeCase node, C context)
    {
        return visitNode(node, context);
    }

    public R visitMergeInsert(MergeInsert node, C context)
    {
        return visitMergeCase(node, context);
    }

    public R visitMergeUpdate(MergeUpdate node, C context)
    {
        return visitMergeCase(node, context);
    }

    public R visitMergeDelete(MergeDelete node, C context)
    {
        return visitMergeCase(node, context);
    }

    public R visitTableElement(TableElement node, C context)
    {
        return visitNode(node, context);
    }

    public R visitLikeClause(LikeClause node, C context)
    {
        return visitTableElement(node, context);
    }

    public R visitProperty(Property node, C context) {
        return visitNode(node, context);
    }

    public R visitQuery(Query node, C context) {
        return visitStatement(node, context);
    }

    public R visitRelation(Relation node, C context) {
        return visitNode(node, context);
    }

    public R visitQueryBody(QueryBody node, C context) {
        return visitRelation(node, context);
    }

    public R visitOrderBy(OrderBy node, C context) {
        return visitNode(node, context);
    }

    public R visitOffset(Offset node, C context) {
        return visitNode(node, context);
    }

    public R visitFetchFirst(FetchFirst node, C context) {
        return visitNode(node, context);
    }

    public R visitLimit(Limit node, C context) {
        return visitNode(node, context);
    }

    public R visitNullIfExpression(NullIfExpression node, C context) {
        return visitExpression(node, context);
    }

    public R visitIfExpression(IfExpression node, C context) {
        return visitExpression(node, context);
    }

    public R visitArithmeticUnary(ArithmeticUnaryExpression node, C context) {
        return visitExpression(node, context);
    }

    public R visitArithmeticBinary(ArithmeticBinaryExpression node, C context) {
        return visitExpression(node, context);
    }

    public R visitBetweenPredicate(BetweenPredicate node, C context) {
        return visitExpression(node, context);
    }

    public R visitCoalesceExpression(CoalesceExpression node, C context) {
        return visitExpression(node, context);
    }

    public R visitLikePredicate(LikePredicate node, C context) {
        return visitExpression(node, context);
    }

    public R visitIsNotNullPredicate(IsNotNullPredicate node, C context) {
        return visitExpression(node, context);
    }

    public R visitIsNullPredicate(IsNullPredicate node, C context) {
        return visitExpression(node, context);
    }

    public R visitComparisonExpression(ComparisonExpression node, C context) {
        return visitExpression(node, context);
    }

    public R visitNotExpression(NotExpression node, C context) {
        return visitExpression(node, context);
    }

    public R visitInListExpression(InListExpression node, C context) {
        return visitExpression(node, context);
    }

    public R visitAllRows(AllRows node, C context) {
        return visitExpression(node, context);
    }

    public R visitWith(With node, C context) {
        return visitNode(node, context);
    }

    public R visitWithQuery(WithQuery node, C context) {
        return visitNode(node, context);
    }

    public R visitSelect(Select node, C context) {
        return visitNode(node, context);
    }

    public R visitRow(Row node, C context) {
        return visitExpression(node, context);
    }

    public R visitTableSubquery(TableSubquery node, C context) {
        return visitQueryBody(node, context);
    }

    public R visitAliasedRelation(AliasedRelation node, C context) {
        return visitRelation(node, context);
    }

    public R visitSampledRelation(SampledRelation node, C context)
    {
        return visitRelation(node, context);
    }

    public R visitTable(Table node, C context) {
        return visitQueryBody(node, context);
    }

    public R visitUnnest(Unnest node, C context) {
        return visitRelation(node, context);
    }

    public R visitLateral(Lateral node, C context)
    {
        return visitRelation(node, context);
    }

    public R visitValues(Values node, C context)
    {
        return visitQueryBody(node, context);
    }

    public R visitJoin(Join node, C context) {
        return visitRelation(node, context);
    }

    public R visitExists(ExistsPredicate node, C context) {
        return visitExpression(node, context);
    }

    public R visitTryExpression(TryExpression node, C context) {
        return visitExpression(node, context);
    }

    public R visitCast(Cast node, C context) {
        return visitExpression(node, context);
    }

    public R visitFieldReference(FieldReference node, C context)
    {
        return visitExpression(node, context);
    }

    public R visitQuerySpecification(QuerySpecification node, C context) {
        return visitQueryBody(node, context);
    }

    public R visitSortItem(SortItem node, C context) {
        return visitNode(node, context);
    }

    public R visitIdentifier(Identifier node, C context) {
        return visitExpression(node, context);
    }

    public R visitInsert(Insert node, C context) {
        return visitStatement(node, context);
    }

    public R visitSelectItem(SelectItem node, C context) {
        return visitNode(node, context);
    }

    public R visitSingleColumn(SingleColumn node, C context) {
        return visitSelectItem(node, context);
    }

    public R visitAllColumns(AllColumns node, C context) {
        return visitSelectItem(node, context);
    }

    public R visitAtTimeZone(AtTimeZone node, C context) {
        return visitExpression(node, context);
    }

    public R visitGroupBy(GroupBy node, C context) {
        return visitNode(node, context);
    }

    public R visitGroupingElement(GroupingElement node, C context) {
        return visitNode(node, context);
    }

    public R visitCube(Cube node, C context) {
        return visitGroupingElement(node, context);
    }

    public R visitGroupingSets(GroupingSets node, C context) {
        return visitGroupingElement(node, context);
    }

    public R visitRollup(Rollup node, C context) {
        return visitGroupingElement(node, context);
    }

    public R visitSimpleGroupBy(SimpleGroupBy node, C context) {
        return visitGroupingElement(node, context);
    }

    public R visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, C context) {
        return visitExpression(node, context);
    }

    public R visitLambdaArgumentDeclaration(LambdaArgumentDeclaration node, C context) {
        return visitExpression(node, context);
    }

    public R visitBindExpression(BindExpression node, C context)
    {
        return visitExpression(node, context);
    }

    public R visitGroupingOperation(GroupingOperation node, C context) {
        return visitExpression(node, context);
    }

    public R visitCurrentCatalog(CurrentCatalog node, C context) {
        return visitExpression(node, context);
    }

    public R visitCurrentSchema(CurrentSchema node, C context) {
        return visitExpression(node, context);
    }

    public R visitCurrentUser(CurrentUser node, C context) {
        return visitExpression(node, context);
    }

    public R visitCurrentTime(CurrentTime node, C context) {
        return visitExpression(node, context);
    }

    public R visitExtract(Extract node, C context) {
        return visitExpression(node, context);
    }

    public R visitCurrentPath(CurrentPath node, C context) {
        return visitExpression(node, context);
    }

    public R visitFormat(Format node, C context) {
        return visitExpression(node, context);
    }

    public R visitDataType(DataType node, C context) {
        return visitExpression(node, context);
    }

    public R visitRowDataType(RowDataType node, C context) {
        return visitDataType(node, context);
    }

    public R visitGenericDataType(GenericDataType node, C context) {
        return visitDataType(node, context);
    }

    public R visitRowField(RowDataType.Field node, C context) {
        return visitNode(node, context);
    }

    public R visitDataTypeParameter(DataTypeParameter node, C context) {
        return visitNode(node, context);
    }

    public R visitNumericTypeParameter(NumericParameter node, C context) {
        return visitDataTypeParameter(node, context);
    }

    public R visitTypeParameter(TypeParameter node, C context) {
        return visitDataTypeParameter(node, context);
    }

    public R visitIntervalDataType(IntervalDayTimeDataType node, C context) {
        return visitDataType(node, context);
    }

    public R visitDateTimeType(DateTimeDataType node, C context) {
        return visitDataType(node, context);
    }

    public R visitArrayConstructor(ArrayConstructor node, C context) {
        return visitExpression(node, context);
    }

    public R visitSubscriptExpression(SubscriptExpression node, C context) {
        return visitExpression(node, context);
    }

    public R visitLogicalExpression(LogicalExpression node, C context) {
        return visitExpression(node, context);
    }

    public R visitSubqueryExpression(SubqueryExpression node, C context) {
        return visitExpression(node, context);
    }

    public R visitInPredicate(InPredicate node, C context) {
        return visitExpression(node, context);
    }

    public R visitSearchedCaseExpression(SearchedCaseExpression node, C context) {
        return visitExpression(node, context);
    }

    public R visitFunctionCall(FunctionCall node, C context) {
        return visitExpression(node, context);
    }

    public R visitProcessingMode(ProcessingMode node, C context) {
        return visitNode(node, context);
    }

    public R visitWindowOperation(WindowOperation node, C context) {
        return visitExpression(node, context);
    }

    public R visitLambdaExpression(LambdaExpression node, C context) {
        return visitExpression(node, context);
    }

    public R visitSimpleCaseExpression(SimpleCaseExpression node, C context) {
        return visitExpression(node, context);
    }

    public R visitWhenClause(WhenClause node, C context) {
        return visitExpression(node, context);
    }

    public R visitLiteral(Literal node, C context) {
        return visitExpression(node, context);
    }

    public R visitDoubleLiteral(DoubleLiteral node, C context) {
        return visitLiteral(node, context);
    }

    public R visitDecimalLiteral(DecimalLiteral node, C context) {
        return visitLiteral(node, context);
    }

    public R visitGenericLiteral(GenericLiteral node, C context) {
        return visitLiteral(node, context);
    }

    public R visitTimeLiteral(TimeLiteral node, C context) {
        return visitLiteral(node, context);
    }

    public R visitTimestampLiteral(TimestampLiteral node, C context) {
        return visitLiteral(node, context);
    }

    public R visitIntervalLiteral(IntervalLiteral node, C context) {
        return visitLiteral(node, context);
    }

    public R visitStringLiteral(StringLiteral node, C context) {
        return visitLiteral(node, context);
    }

    public R visitCharLiteral(CharLiteral node, C context) {
        return visitLiteral(node, context);
    }

    public R visitBinaryLiteral(BinaryLiteral node, C context) {
        return visitLiteral(node, context);
    }

    public R visitBooleanLiteral(BooleanLiteral node, C context) {
        return visitLiteral(node, context);
    }

    public R visitNullLiteral(NullLiteral node, C context) {
        return visitLiteral(node, context);
    }

    public R visitLongLiteral(LongLiteral node, C context) {
        return visitLiteral(node, context);
    }

    public R visitParameter(Parameter node, C context)
    {
        return visitExpression(node, context);
    }

    public R visitWindowReference(WindowReference node, C context) {
        return visitNode(node, context);
    }

    public R visitWindowSpecification(WindowSpecification node, C context) {
        return visitNode(node, context);
    }

    public R visitWindowDefinition(WindowDefinition node, C context) {
        return visitNode(node, context);
    }

    public R visitWindowFrame(WindowFrame node, C context) {
        return visitNode(node, context);
    }

    public R visitFrameBound(FrameBound node, C context) {
        return visitNode(node, context);
    }

    public R visitMeasureDefinition(MeasureDefinition node, C context) {
        return visitNode(node, context);
    }

    public R visitSkipTo(SkipTo node, C context) {
        return visitNode(node, context);
    }

    public R visitPatternSearchMode(PatternSearchMode node, C context) {
        return visitNode(node, context);
    }

    public R visitSubsetDefinition(SubsetDefinition node, C context) {
        return visitNode(node, context);
    }

    public R visitVariableDefinition(VariableDefinition node, C context) {
        return visitNode(node, context);
    }

    public R visitPatternRecognitionRelation(PatternRecognitionRelation node, C context) {
        return visitRelation(node, context);
    }

    public R visitRowPattern(RowPattern node, C context) {
        return visitNode(node, context);
    }

    public R visitPatternAlternation(PatternAlternation node, C context) {
        return visitRowPattern(node, context);
    }

    public R visitPatternConcatenation(PatternConcatenation node, C context) {
        return visitRowPattern(node, context);
    }

    public R visitQuantifiedPattern(QuantifiedPattern node, C context) {
        return visitRowPattern(node, context);
    }

    public R visitAnchorPattern(AnchorPattern node, C context) {
        return visitRowPattern(node, context);
    }

    public R visitEmptyPattern(EmptyPattern node, C context) {
        return visitRowPattern(node, context);
    }

    public R visitExcludedPattern(ExcludedPattern node, C context) {
        return visitRowPattern(node, context);
    }

    public R visitPatternPermutation(PatternPermutation node, C context) {
        return visitRowPattern(node, context);
    }

    public R visitPatternVariable(PatternVariable node, C context) {
        return visitRowPattern(node, context);
    }

    public R visitPatternQuantifier(PatternQuantifier node, C context) {
        return visitNode(node, context);
    }

    public R visitZeroOrMoreQuantifier(ZeroOrMoreQuantifier node, C context) {
        return visitPatternQuantifier(node, context);
    }

    public R visitOneOrMoreQuantifier(OneOrMoreQuantifier node, C context) {
        return visitPatternQuantifier(node, context);
    }

    public R visitZeroOrOneQuantifier(ZeroOrOneQuantifier node, C context) {
        return visitPatternQuantifier(node, context);
    }

    public R visitRangeQuantifier(RangeQuantifier node, C context) {
        return visitPatternQuantifier(node, context);
    }

    public R visitQueryPeriod(QueryPeriod node, C context) {
        return visitNode(node, context);
    }

    public R visitSymbolReference(SymbolReference node, C context) {
        return visitExpression(node, context);
    }

    public R visitLabelDereference(LabelDereference node, C context) {
        return visitExpression(node, context);
    }

    public R visitSetOperation(SetOperation node, C context)
    {
        return visitQueryBody(node, context);
    }

    public R visitUnion(Union node, C context)
    {
        return visitSetOperation(node, context);
    }

    public R visitIntersect(Intersect node, C context)
    {
        return visitSetOperation(node, context);
    }

    public R visitExcept(Except node, C context)
    {
        return visitSetOperation(node, context);
    }

    public R visitPathSpecification(PathSpecification node, C context)
    {
        return visitNode(node, context);
    }

    public R visitPathElement(PathElement node, C context)
    {
        return visitNode(node, context);
    }
}
