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

package org.apache.paimon.flink.source;

import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.flink.sink.FlinkTableSink;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateRemapper;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.NestedFieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * ITCase for predicates on a field nested inside a row: that pushing them down leaves every result
 * unchanged, that a real SQL plan hands them to the scan, and that the scan reads less for it.
 *
 * <p>The scan digest in {@code EXPLAIN} is deliberately not asserted on. {@link
 * FlinkTableSource#applyFilters} reports every filter as accepted whether or not it could be
 * converted, so {@code filter=[...]} reads the same either way. Instead, the source a real SQL plan
 * ends up with is taken out of the translated job, and its own {@link ReadBuilder} is used to count
 * what the scan returns.
 */
public class NestedFieldFilterPushDownITCase extends CatalogITCaseBase {

    @Override
    public List<String> ddl() {
        return Arrays.asList(
                "CREATE TABLE NT (pk INT, s ROW<a INT, b STRING>, d DOUBLE)",
                "CREATE TABLE FT (pk INT, s ROW<d DOUBLE, f FLOAT>, d DOUBLE)",
                "CREATE TABLE PPT (dt STRING, pk INT, s ROW<a INT>,"
                        + " PRIMARY KEY (dt, pk) NOT ENFORCED) PARTITIONED BY (dt)"
                        + " WITH ('bucket' = '1')");
    }

    // ------------------------------------------------------------------------------------
    // results do not change
    // ------------------------------------------------------------------------------------

    @Test
    public void testNestedPredicateKeepsMatchingRows() {
        batchSql(
                "INSERT INTO NT VALUES (1, ROW(7, 'abc'), 1.5), (2, ROW(8, 'xyz'), 2.5),"
                        + " (3, CAST(NULL AS ROW<a INT, b STRING>), 3.5)");

        assertThat(batchSql("SELECT pk FROM NT WHERE s.a = 7")).containsExactly(Row.of(1));
        assertThat(batchSql("SELECT pk FROM NT WHERE s.a > 7")).containsExactly(Row.of(2));
        assertThat(batchSql("SELECT pk FROM NT WHERE s.a IN (7, 8)"))
                .containsExactlyInAnyOrder(Row.of(1), Row.of(2));
        assertThat(batchSql("SELECT pk FROM NT WHERE s.b LIKE 'ab%%'")).containsExactly(Row.of(1));
        assertThat(batchSql("SELECT pk FROM NT WHERE s.a IS NULL")).containsExactly(Row.of(3));
        assertThat(batchSql("SELECT pk FROM NT WHERE s.a IS NOT NULL"))
                .containsExactlyInAnyOrder(Row.of(1), Row.of(2));
    }

    @Test
    public void testNegatedNestedPredicateKeepsMatchingRows() {
        batchSql("INSERT INTO NT VALUES (1, ROW(7, 'abc'), 1.5), (2, ROW(8, 'xyz'), 2.5)");

        assertThat(batchSql("SELECT pk FROM NT WHERE NOT (s.a = 7)")).containsExactly(Row.of(2));
        assertThat(batchSql("SELECT pk FROM NT WHERE s.a NOT IN (7)")).containsExactly(Row.of(2));
        assertThat(batchSql("SELECT pk FROM NT WHERE s.a NOT BETWEEN 1 AND 7"))
                .containsExactly(Row.of(2));
    }

    @Test
    public void testNestedAndTopLevelPredicatesCombine() {
        batchSql(
                "INSERT INTO NT VALUES (1, ROW(7, 'abc'), 1.5), (2, ROW(7, 'xyz'), 2.5),"
                        + " (3, ROW(8, 'abc'), 3.5)");

        assertThat(batchSql("SELECT pk FROM NT WHERE s.a = 7 AND pk = 2"))
                .containsExactly(Row.of(2));
        assertThat(batchSql("SELECT pk FROM NT WHERE s.a = 8 OR pk = 1"))
                .containsExactlyInAnyOrder(Row.of(1), Row.of(3));
    }

    // ------------------------------------------------------------------------------------
    // a real SQL plan hands the predicate to the scan, and the scan prunes on it
    // ------------------------------------------------------------------------------------

    /**
     * Two files whose {@code s.a} ranges do not overlap. The count is what the planned source's own
     * scan returns, with no row-by-row filtering, so it can only drop below the table's six rows if
     * the predicate reached the scan and whole row groups were skipped.
     */
    @Test
    public void testRealPlanPushesNestedPredicateToScan() throws Exception {
        writeTwoFilesWithDisjointNestedValues();

        assertThat(rowsScannedByPlan("SELECT pk FROM NT")).isEqualTo(6);
        // 3, not 1: the second file's row group is skipped whole and the first file's three rows
        // all come back. A 1 would mean rows were filtered individually rather than pruned.
        assertThat(rowsScannedByPlan("SELECT pk FROM NT WHERE s.a = 2")).isEqualTo(3);
        assertThat(rowsScannedByPlan("SELECT pk FROM NT WHERE s.a = 9999")).isZero();
        assertThat(rowsScannedByPlan("SELECT pk FROM NT WHERE s.a > 0")).isEqualTo(6);

        // and pruning never costs a matching row
        assertThat(batchSql("SELECT pk FROM NT WHERE s.a = 2")).containsExactly(Row.of(2));
        assertThat(batchSql("SELECT pk FROM NT WHERE s.a = 9999")).isEmpty();
    }

    // ------------------------------------------------------------------------------------
    // floating point: nested comparisons are left to Flink
    // ------------------------------------------------------------------------------------

    /** Flink SQL treats {@code -0.0 = 0.0} as true; the nested row must not be pruned away. */
    @Test
    public void testNestedDoubleSignedZeroKeepsMatchingRows() throws Exception {
        writeNegativeZero();
        assertThat(batchSql("SELECT pk FROM FT WHERE s.d = 0.0")).containsExactly(Row.of(1));
        assertThat(batchSql("SELECT pk FROM FT WHERE s.f = 0.0")).containsExactly(Row.of(1));
    }

    /**
     * Two files whose nested floating-point values do not overlap, so any of these predicates would
     * prune one of them if it reached the scan. None may: the scan must return every row. {@code IS
     * NULL} is not a comparison and is still pushed down, so it does prune.
     */
    @Test
    public void testNestedFloatingPointComparisonsAreNotPushedToScan() throws Exception {
        batchSql(
                "INSERT INTO FT VALUES (1, ROW(1.0, CAST(1.0 AS FLOAT)), 1.0),"
                        + " (2, ROW(2.0, CAST(2.0 AS FLOAT)), 2.0)");
        batchSql(
                "INSERT INTO FT VALUES (3, ROW(1001.0, CAST(1001.0 AS FLOAT)), 3.0),"
                        + " (4, ROW(1002.0, CAST(1002.0 AS FLOAT)), 4.0)");

        for (String where :
                new String[] {
                    "s.d = 1.0",
                    "s.d <> 1.0",
                    "s.d < 5.0",
                    "s.d <= 5.0",
                    "s.d > 1000.0",
                    "s.d >= 1000.0",
                    "5.0 > s.d",
                    "s.d IN (1.0, 2.0)",
                    "s.d BETWEEN 0.0 AND 5.0",
                    "s.f = 1.0",
                    "s.f < 5.0"
                }) {
            assertThat(rowsScannedByPlan("SELECT pk FROM FT WHERE " + where))
                    .as("%s must be left to Flink, not pushed to the scan", where)
                    .isEqualTo(4);
        }

        assertThat(rowsScannedByPlan("SELECT pk FROM FT WHERE s.d IS NULL")).isZero();

        // and Flink still evaluates them correctly
        assertThat(batchSql("SELECT pk FROM FT WHERE s.d = 1.0")).containsExactly(Row.of(1));
        assertThat(batchSql("SELECT pk FROM FT WHERE s.d BETWEEN 0.0 AND 5.0"))
                .containsExactlyInAnyOrder(Row.of(1), Row.of(2));
    }

    // ------------------------------------------------------------------------------------
    // field ids: the predicate carries the table's own ids
    // ------------------------------------------------------------------------------------

    /**
     * On an evolved schema, field ids are not the ones a type round-tripped through Flink would
     * number: here {@code s.c} is added after a column was dropped. A masked read remaps the filter
     * onto the table's read schema ({@code ReadTransform} keeps the conjuncts on masked columns via
     * {@code TableQueryAuthResult.retainFields}, then {@code PredicateRemapper.remap}) and the
     * nested transform checks the ids it carries against the table's.
     *
     * <p>The predicate is taken from {@link FlinkTableSource#applyFilters} itself, so this covers
     * how the source builds it, not only the converter.
     */
    @Test
    public void testNestedPredicateKeepsTableFieldIdsOnEvolvedSchema() throws Exception {
        evolveNestedRow();
        FileStoreTable table = paimonTable("NT");

        DataTableSource source =
                new DataTableSource(
                        ObjectIdentifier.of("PAIMON", "default", "NT"), table, false, null);
        source.applyFilters(Collections.singletonList(nestedEqualsInt("s", "c", 1)));
        assertThat(source.predicate).as("s.c = 1 must be converted").isNotNull();

        Predicate onMaskedColumn =
                TableQueryAuthResult.retainFields(source.predicate, Collections.singleton("s"));
        assertThatCode(() -> PredicateRemapper.remap(onMaskedColumn, table.rowType()))
                .doesNotThrowAnyException();
    }

    /** Queries on an evolved nested row keep returning the right rows. */
    @Test
    public void testNestedPredicateOnEvolvedSchemaKeepsMatchingRows() {
        batchSql("INSERT INTO NT VALUES (1, ROW(7, 'x'), 1.0)");
        evolveNestedRow();
        batchSql("INSERT INTO NT VALUES (2, ROW(8, 'y', 1), 2.0), (3, ROW(9, 'z', 2), 3.0)");

        assertThat(batchSql("SELECT pk FROM NT WHERE s.c = 1")).containsExactly(Row.of(2));
        // rows written before s.c existed read it as null
        assertThat(batchSql("SELECT pk FROM NT WHERE s.c IS NULL")).containsExactly(Row.of(1));
        assertThat(batchSql("SELECT pk FROM NT WHERE s.a = 7")).containsExactly(Row.of(1));
    }

    // ------------------------------------------------------------------------------------
    // DELETE: a nested predicate is never executed by Paimon as a partition drop
    // ------------------------------------------------------------------------------------

    /**
     * {@code applyDeleteFilters} returning true hands the whole DELETE to Paimon, which can only
     * execute it by dropping partitions. A nested predicate must never qualify, alone or next to a
     * partition key, or rows it does not match would be deleted with the partition.
     *
     * <p>Called directly rather than through {@code DELETE FROM ... WHERE s.a = ...}: Flink's own
     * delete push-down resolves such a filter without a row type and fails in the planner before
     * this sink is ever asked.
     */
    @Test
    public void testNestedDeleteFilterIsNeverExecutedByPaimon() throws Exception {
        ObjectIdentifier identifier = ObjectIdentifier.of("PAIMON", "default", "PPT");
        ResolvedExpression partition =
                CallExpression.permanent(
                        BuiltInFunctionDefinitions.EQUALS,
                        Arrays.asList(
                                new FieldReferenceExpression("dt", DataTypes.STRING(), 0, 0),
                                new ValueLiteralExpression("p1")),
                        DataTypes.BOOLEAN());
        ResolvedExpression nested = nestedEqualsInt("s", "a", 2);

        // control: a partition key alone is handed to Paimon as a partition drop
        assertThat(
                        new FlinkTableSink(identifier, paimonTable("PPT"), null)
                                .applyDeleteFilters(Collections.singletonList(partition)))
                .isTrue();

        assertThat(
                        new FlinkTableSink(identifier, paimonTable("PPT"), null)
                                .applyDeleteFilters(Collections.singletonList(nested)))
                .isFalse();
        assertThat(
                        new FlinkTableSink(identifier, paimonTable("PPT"), null)
                                .applyDeleteFilters(Arrays.asList(partition, nested)))
                .as("a nested predicate next to a partition key must not drop the partition")
                .isFalse();
    }

    // ------------------------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------------------------

    private void writeTwoFilesWithDisjointNestedValues() {
        // two commits, so the rows land in two files whose s.a ranges do not overlap
        batchSql(
                "INSERT INTO NT VALUES (1, ROW(1, 'x'), 1.0), (2, ROW(2, 'x'), 1.0),"
                        + " (3, ROW(3, 'x'), 1.0)");
        batchSql(
                "INSERT INTO NT VALUES (4, ROW(1001, 'y'), 2.0), (5, ROW(1002, 'y'), 2.0),"
                        + " (6, ROW(1003, 'y'), 2.0)");
    }

    private void evolveNestedRow() {
        // leaves a gap in the field ids, then adds s.c after it
        batchSql("ALTER TABLE NT ADD extra INT");
        batchSql("ALTER TABLE NT DROP extra");
        batchSql("ALTER TABLE NT MODIFY s ROW<a INT, b STRING, c INT>");
    }

    private static ResolvedExpression nestedEqualsInt(String root, String leaf, int literal) {
        return CallExpression.permanent(
                BuiltInFunctionDefinitions.EQUALS,
                Arrays.asList(
                        new NestedFieldReferenceExpression(
                                new String[] {root, leaf}, new int[] {1, 2}, DataTypes.INT()),
                        new ValueLiteralExpression(literal)),
                DataTypes.BOOLEAN());
    }

    /**
     * Writes one row whose nested and top-level floating-point values are all negative zero.
     * Written through the table API because SQL cannot express it: {@code CAST('-0.0' AS DOUBLE)}
     * goes through a decimal, which has no negative zero, and comes back as {@code 0.0}.
     */
    private void writeNegativeZero() throws Exception {
        FileStoreTable table = paimonTable("FT");
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            write.write(GenericRow.of(1, GenericRow.of(-0.0d, -0.0f), -0.0d));
            commit.commit(write.prepareCommit());
        }

        // it really is stored as negative zero
        List<InternalRow> rows = new ArrayList<>();
        ReadBuilder readBuilder = table.newReadBuilder();
        readBuilder
                .newRead()
                .createReader(readBuilder.newScan().plan())
                .forEachRemaining(rows::add);
        assertThat(rows).hasSize(1);
        InternalRow nested = rows.get(0).getRow(1, 2);
        assertThat(Double.doubleToRawLongBits(nested.getDouble(0)))
                .isEqualTo(Double.doubleToRawLongBits(-0.0d));
        assertThat(Float.floatToRawIntBits(nested.getFloat(1)))
                .isEqualTo(Float.floatToRawIntBits(-0.0f));
    }

    /**
     * Plans {@code sql} the way a real query is planned, takes the source out of the translated
     * job, and counts what that source's own scan returns. Nothing filters row by row here: a count
     * below the table's size means the predicate reached the scan and pruned.
     */
    private int rowsScannedByPlan(String sql) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        StreamTableEnvironment planner = StreamTableEnvironment.create(env);
        planner.registerCatalog("PAIMON", tEnv.getCatalog("PAIMON").get());
        planner.useCatalog("PAIMON");

        FlinkSource source =
                plannedSource(planner.toDataStream(planner.sqlQuery(sql)).getTransformation());
        ReadBuilder readBuilder = source.readBuilder;
        AtomicInteger count = new AtomicInteger();
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
            reader.forEachRemaining(row -> count.incrementAndGet());
        }
        return count.get();
    }

    private static FlinkSource plannedSource(Transformation<?> transformation) throws Exception {
        if (transformation instanceof SourceTransformation) {
            Source<?, ?, ?> source = ((SourceTransformation<?, ?, ?>) transformation).getSource();
            if (source instanceof PaimonDataStreamSource) {
                // the wrapper keeps the source it delegates to private
                Field inner = PaimonDataStreamSource.class.getDeclaredField("source");
                inner.setAccessible(true);
                source = (Source<?, ?, ?>) inner.get(source);
            }
            return (FlinkSource) source;
        }
        for (Transformation<?> input : transformation.getInputs()) {
            FlinkSource found = plannedSource(input);
            if (found != null) {
                return found;
            }
        }
        return null;
    }
}
