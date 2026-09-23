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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.PredicateConverter;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.NestedFieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ITCase for predicates on a field nested inside a row: that pushing them down leaves every result
 * unchanged, and that Paimon really does read less because of them.
 *
 * <p>The query plan is deliberately not asserted on. {@link
 * org.apache.paimon.flink.source.FlinkTableSource#applyFilters} reports every filter as accepted
 * whether or not it could be converted, so {@code filter=[...]} in the scan digest reads the same
 * either way.
 */
public class NestedFieldFilterPushDownITCase extends CatalogITCaseBase {

    @Override
    public List<String> ddl() {
        return Collections.singletonList(
                "CREATE TABLE NT (pk INT, s ROW<a INT, b STRING>, d DOUBLE)");
    }

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

    /**
     * The predicate must actually prune. Two files are written whose nested values do not overlap;
     * reading with a predicate that only one of them can satisfy must come back with fewer rows
     * than the table holds.
     *
     * <p>Rows are counted through {@link ReadBuilder#newRead()} without {@code executeFilter()}, so
     * nothing filters row by row: what is counted is what the format handed back after pruning, and
     * dropping below the table's row count is only possible if whole row groups were skipped.
     */
    @Test
    public void testNestedPredicatePrunesReads() throws Exception {
        // two commits, so the rows land in two files whose s.a ranges do not overlap
        batchSql(
                "INSERT INTO NT VALUES (1, ROW(1, 'x'), 1.0), (2, ROW(2, 'x'), 1.0),"
                        + " (3, ROW(3, 'x'), 1.0)");
        batchSql(
                "INSERT INTO NT VALUES (4, ROW(1001, 'y'), 2.0), (5, ROW(1002, 'y'), 2.0),"
                        + " (6, ROW(1003, 'y'), 2.0)");

        FileStoreTable table = paimonTable("NT");
        assertThat(rowsRead(table, null)).isEqualTo(6);

        // s.a = 2 can only be in the first file; the second one holds 1001..1003
        Predicate predicate = nestedEqual(table, 2);

        // 3, not 1: the second file's row group is skipped whole, and the first file's three rows
        // all come back. A 1 here would mean rows were filtered individually rather than pruned.
        assertThat(rowsRead(table, predicate))
                .as("the file holding s.a in 1001..1003 must be pruned away")
                .isEqualTo(3);

        // a value no file can hold prunes both of them
        assertThat(rowsRead(table, nestedEqual(table, 9999)))
                .as("no file holds s.a = 9999, so every row group is skipped")
                .isZero();

        // a range spanning both files prunes neither
        assertThat(
                        rowsRead(
                                table,
                                nestedPredicate(table, BuiltInFunctionDefinitions.GREATER_THAN, 0)))
                .as("s.a > 0 matches both files, so nothing is pruned")
                .isEqualTo(6);

        // and the matching row still survives the pruning
        assertThat(batchSql("SELECT pk FROM NT WHERE s.a = 2")).containsExactly(Row.of(2));
        assertThat(batchSql("SELECT pk FROM NT WHERE s.a = 9999")).isEmpty();
    }

    private Predicate nestedEqual(FileStoreTable table, int literal) {
        return nestedPredicate(table, BuiltInFunctionDefinitions.EQUALS, literal);
    }

    /** The predicate Paimon gets for {@code s.a <op> literal}, as the Flink converter builds it. */
    private Predicate nestedPredicate(
            FileStoreTable table, BuiltInFunctionDefinition func, int literal) {
        ResolvedExpression call =
                CallExpression.permanent(
                        func,
                        Arrays.asList(
                                new NestedFieldReferenceExpression(
                                        new String[] {"s", "a"}, new int[] {1, 0}, DataTypes.INT()),
                                new ValueLiteralExpression(literal)),
                        DataTypes.BOOLEAN());
        return PredicateConverter.convert(
                        LogicalTypeConversion.toLogicalType(table.rowType()), call)
                .orElseThrow(() -> new AssertionError("nested predicate was not converted"));
    }

    /** Rows the format returns for {@code predicate}, without any row-level filtering. */
    private int rowsRead(FileStoreTable table, Predicate predicate) throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder();
        if (predicate != null) {
            readBuilder = readBuilder.withFilter(predicate);
        }
        AtomicInteger count = new AtomicInteger();
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
            reader.forEachRemaining(row -> count.incrementAndGet());
        }
        return count.get();
    }
}
