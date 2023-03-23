/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.presto;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;
import org.testng.SkipException;

/** The test of PrestoDistributedQuery. */
public class PrestoDistributedQueryTest extends AbstractTestDistributedQueries {

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        return PrestoQueryRunner.createPrestoQueryRunner(ImmutableMap.of());
    }

    @Override
    protected boolean supportsViews() {
        return false;
    }

    // The following tests require a table creation method,the pr of presto writer will do this

    @Override
    public void testAddColumn() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCreateTableAsSelect() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDelete() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDropColumn() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testExplainAnalyze() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testExplainAnalyzeVerbose() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testInsert() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testInsertIntoNotNullColumn() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testQueryLoggingCount() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testRenameColumn() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testRenameTable() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testSymbolAliasing() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testMergeEmptyNonEmptyApproxSetWithDifferentMaxError() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testTableSampleSystemBoundaryValues() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testWrittenStats() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testAccessControl() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testAggregationOverUnknown() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testAliasedInInlineView() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testAndInFilter() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testApproxPercentile() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testApproxSetBigint() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testApproxSetBigintGroupBy() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testApproxSetBigintWithMaxError() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testApproxSetDouble() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testApproxSetDoubleGroupBy() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testApproxSetDoubleWithMaxError() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testApproxSetGroupByWithNulls() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testApproxSetGroupByWithOnlyNullsInOneGroup() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testApproxSetOnlyNulls() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testApproxSetVarchar() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testApproxSetVarcharGroupBy() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testApproxSetVarcharWithMaxError() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testApproxSetWithNulls() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testArithmeticNegation() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testArrayAgg() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testArrayShuffle() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testArrays() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testAssignUniqueId() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testAverageAll() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCaseInsensitiveAliasedRelation() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCaseInsensitiveAttribute() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCaseNoElse() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCaseNoElseInconsistentResultType() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCast() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testChainedUnionsWithOrder() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testColumnAliases() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testComparisonWithLike() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testComplexQuery() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCorrelatedExistsSubqueries() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCorrelatedExistsSubqueriesWithEqualityPredicatesInWhere() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCorrelatedExistsSubqueriesWithPrunedCorrelationSymbols() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCorrelatedInPredicateSubqueries() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCorrelatedNonAggregationScalarSubqueries() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCorrelatedScalarSubqueries() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCorrelatedScalarSubqueriesWithScalarAggregation() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCorrelatedScalarSubqueriesWithScalarAggregationAndEqualityPredicatesInWhere() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testTwoCorrelatedExistsSubqueries() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCountAll() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCountColumn() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCustomAdd() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCustomRank() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCustomSum() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDefaultExplainGraphvizFormat() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDefaultExplainTextFormat() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDereferenceInComparison() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDereferenceInFunctionCall() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDereferenceInSubquery() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDescribeInput() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDescribeInputWithAggregation() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDescribeInputNoParameters() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDescribeOutput() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDescribeOutputNamedAndUnnamed() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDescribeOutputNonSelect() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDescribeOutputOnAliasedColumnsAndExpressions() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDistinct() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDistinctFrom() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDistinctHaving() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDistinctLimit() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDistinctLimitInternal(Session session) {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDistinctLimitWithHashBasedDistinctLimitEnabled() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDistinctLimitWithQuickDistinctLimitEnabled() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDistinctMultipleFields() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDistinctWithOrderBy() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDistinctWithOrderByNotInSelect() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDistributedExplain() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDistributedExplainGraphvizFormat() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDistributedExplainJsonFormat() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDistributedExplainTextFormat() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDuplicateFields() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testExcept() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testExceptWithAggregation() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testExchangeWithProjectionPushDown() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testExecuteUsingWithSubquery() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testExistsSubquery() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testExistsSubqueryWithGroupBy() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testExplainExecute() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testExplainExecuteWithUsing() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testExplainOfExplain() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testExplainOfExplainAnalyze() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testFilterPushdownWithAggregation() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testGroupByKeyPredicatePushdown() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testGroupByOrderByLimit() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testGroupingInTableSubquery() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testHaving() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testHaving2() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testHaving3() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testIOExplain() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testHavingWithoutGroupBy() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testIfExpression() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testIn() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testInformationSchemaFiltering() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testInformationSchemaUppercaseName() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testLimitInInlineView() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testIntersect() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testIntersectWithAggregation() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testInvalidCastInMultilineQuery() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testInvalidColumn() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testKeyBasedSampling() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testKeyBasedSamplingFunctionError() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testLargeBytecode() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testLargeIn() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testLimit() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testLimitAll() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testLimitIntMax() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testLimitPushDown() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testLimitWithAggregation() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testLimitZero() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testLogicalExplain() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testLogicalExplainGraphvizFormat() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testLogicalExplainJsonFormat() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testLogicalExplainTextFormat() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testMaps() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testMaxBy() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testMaxByN() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testMaxMinStringWithNulls() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testMergeEmptyApproxSet() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testMergeEmptyNonEmptyApproxSet() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testMergeEmptyNonEmptyApproxSetWithSameMaxError() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testMergeHyperLogLog() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testMergeHyperLogLogGroupBy() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testMergeHyperLogLogGroupByWithNulls() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testMergeHyperLogLogOnlyNulls() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testMergeHyperLogLogWithNulls() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testMinBy() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testMinByN() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testMixedWildcards() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testMultiColumnUnionAll() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testMultipleWildcards() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testNonDeterministic() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testNonDeterministicAggregationPredicatePushdown() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testNonDeterministicProjection() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testNonDeterministicTableScanPredicatePushdown() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testOffsetEmptyResult() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testP4ApproxSetBigint() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testP4ApproxSetBigintGroupBy() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testP4ApproxSetDouble() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testP4ApproxSetDoubleGroupBy() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testP4ApproxSetGroupByWithNulls() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testP4ApproxSetGroupByWithOnlyNullsInOneGroup() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testP4ApproxSetOnlyNulls() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testP4ApproxSetVarchar() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testP4ApproxSetVarcharGroupBy() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testP4ApproxSetWithNulls() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testPredicatePushdown() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testPruningCountAggregationOverScalar() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testQualifiedWildcard() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testQualifiedWildcardFromAlias() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testQualifiedWildcardFromInlineView() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testWithQualifiedPrefix() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testQuotedIdentifiers() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testReduceAggWithArrayConcat() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testRedundantProjection() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testReferenceToWithQueryInFromClause() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testRepeatedOutputs() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testRepeatedOutputs2() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testRollupOverUnion() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testInlineView() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testInlineViewWithProjections() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testQuantifiedComparison() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testRepeatedAggregations() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testRowNumberLimit() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testRowNumberNoOptimization() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testRowNumberPartitionedFilter() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testRowNumberPropertyDerivation() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testRowNumberUnpartitionedFilter() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testRowNumberUnpartitionedFilterLimit() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testRowSubscript() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testSamplingJoinChain() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testScalarSubquery() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testScalarSubqueryWithGroupBy() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testSelectCaseInsensitive() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testSelectColumnOfNulls() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testSelectWithComparison() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testShowColumns() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testShowSchemasLikeWithEscape() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testShowTables() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testShowTablesFrom() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testSetAgg() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testShowTablesLike() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testShowTablesLikeWithEscape() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testStdDev() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testStdDevPop() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testSubqueriesWithDisjunction() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testSubqueryBody() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testSubqueryBodyDoubleOrderby() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testSubqueryBodyOrderLimit() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testSubqueryBodyProjectedOrderby() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testSubqueryUnion() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testSwitchOptimization() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testTableAsSubquery() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testTableQuery() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testTableQueryInUnion() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testTableQueryOrderLimit() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testTableSampleBernoulli() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testTableSampleBernoulliBoundaryValues() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testTopNByMultipleFields() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testTopNPartitionedWindow() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testTopNPartitionedWindowWithEqualityFilter() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testTopNUnpartitionedLargeWindow() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testTopNUnpartitionedWindow() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testTopNUnpartitionedWindowWithCompositeFilter() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testTopNUnpartitionedWindowWithEqualityFilter() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testUnionWithTopN() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testTry() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testUnaliasedSubqueries() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testUnaliasedSubqueries1() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testUnion() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testUnionAll() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testUnionAllPredicateMoveAroundWithOverlappingProjections() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testUnionDistinct() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testUnionRequiringCoercion() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testUnionWithAggregation() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testUnionWithAggregationAndTableScan() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testUnionWithFilterNotInSelect() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testUnionWithProjectionPushDown() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testUnionWithUnionAndAggregation() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testUnnest() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testVariancePop() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testVariance() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testWhereNull() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testWildcard() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testWildcardFromSubquery() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testWith() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testWithAliased() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testWithChaining() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testWithColumnAliasing() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testWithNestedSubqueries() {
        throw new SkipException("TODO: test not implemented yet");
    }
}
