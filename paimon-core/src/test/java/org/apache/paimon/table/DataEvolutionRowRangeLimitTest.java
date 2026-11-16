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

package org.apache.paimon.table;

import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.DataEvolutionBatchScan;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.SOURCE_SPLIT_TARGET_SIZE;
import static org.apache.paimon.predicate.SortValue.NullOrdering.NULLS_LAST;
import static org.apache.paimon.predicate.SortValue.SortDirection.ASCENDING;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests row-range filtering together with LIMIT and TopN pruning. */
class DataEvolutionRowRangeLimitTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testLimitKeepsMatchingRowsFromLaterFiles() throws Exception {
        Table table = createTable();
        writeFile(table, 0);
        writeFile(table, 256);

        List<Range> ranges = Arrays.asList(new Range(0, 0), new Range(256, 256));
        assertThat(readIds(table.newReadBuilder(), Integer.MAX_VALUE)).hasSize(512);
        assertThat(readIds(table.newReadBuilder().withRowRanges(ranges), Integer.MAX_VALUE))
                .containsExactlyInAnyOrder(0, 256);
        assertThat(readIds(table.newReadBuilder().withRowRanges(ranges).withLimit(2), 2))
                .containsExactlyInAnyOrder(0, 256);
    }

    @Test
    void testLimitWithoutRowRangesStillPrunesFiles() throws Exception {
        Table table = createTable();
        writeFile(table, 0);
        writeFile(table, 256);

        ReadBuilder read = table.newReadBuilder().withLimit(2);
        TableScan.Plan plan = read.newScan().plan();
        assertThat(plan.splits().stream().mapToLong(Split::rowCount).sum()).isEqualTo(256);
        assertThat(readIds(read, plan, 2))
                .hasSize(2)
                .doesNotHaveDuplicates()
                .allMatch(id -> id >= 0 && id < 512);
    }

    @Test
    void testLimitWithDataFilter() throws Exception {
        Table table = createTable();
        writeFile(table, 0);
        writeFile(table, 256);

        ReadBuilder read =
                table.newReadBuilder()
                        .withFilter(new PredicateBuilder(table.rowType()).greaterOrEqual(0, 510))
                        .withLimit(2);
        assertThat(readIds(read, 2)).containsExactlyInAnyOrder(510, 511);
    }

    @Test
    void testLimitWithExternalIndexResult() throws Exception {
        Table table = createTable();
        writeFile(table, 0);
        writeFile(table, 256);

        ReadBuilder read = table.newReadBuilder().withLimit(2);
        DataEvolutionBatchScan scan = (DataEvolutionBatchScan) read.newScan();
        scan.withGlobalIndexResult(
                GlobalIndexResult.fromRanges(Arrays.asList(new Range(0, 0), new Range(256, 256))));
        assertThat(readIds(read, scan.plan(), 2)).containsExactlyInAnyOrder(0, 256);
    }

    @Test
    void testLimitWithNoMatchingRows() throws Exception {
        Table table = createTable();
        writeFile(table, 0);
        assertThat(
                        readIds(
                                table.newReadBuilder()
                                        .withRowRanges(Collections.emptyList())
                                        .withLimit(2),
                                2))
                .isEmpty();
        assertThat(
                        readIds(
                                table.newReadBuilder()
                                        .withRowRanges(
                                                Collections.singletonList(new Range(256, 257)))
                                        .withLimit(2),
                                2))
                .isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testTopNUsesSelectedRows(boolean pushLimit) throws Exception {
        Table table = createTable();
        writeFile(table, 0);
        writeFile(table, 100);
        table = table.copy(Collections.singletonMap(SOURCE_SPLIT_TARGET_SIZE.key(), "1 b"));
        assertThat(table.newReadBuilder().newScan().plan().splits()).hasSize(2);

        List<Range> ranges = Arrays.asList(new Range(255, 255), new Range(256, 256));
        assertThat(readIds(table.newReadBuilder().withRowRanges(ranges), Integer.MAX_VALUE))
                .containsExactlyInAnyOrder(255, 100);
        ReadBuilder read =
                table.newReadBuilder()
                        .withRowRanges(ranges)
                        .withTopN(
                                new TopN(
                                        new FieldRef(0, "id", DataTypes.INT()),
                                        ASCENDING,
                                        NULLS_LAST,
                                        1));
        if (pushLimit) {
            read.withLimit(1);
        }
        // The consumer orders the candidates and applies the final TopN.
        assertThat(
                        readIds(read, Integer.MAX_VALUE).stream()
                                .sorted()
                                .limit(1)
                                .collect(Collectors.toList()))
                .containsExactly(100);
    }

    @Test
    void testTopNWithoutRowRangesStillPrunesFiles() throws Exception {
        Table table = createTable();
        writeFile(table, 0);
        writeFile(table, 256);
        table = table.copy(Collections.singletonMap(SOURCE_SPLIT_TARGET_SIZE.key(), "1 b"));
        assertThat(table.newReadBuilder().newScan().plan().splits()).hasSize(2);

        ReadBuilder read =
                table.newReadBuilder()
                        .withTopN(
                                new TopN(
                                        new FieldRef(0, "id", DataTypes.INT()),
                                        ASCENDING,
                                        NULLS_LAST,
                                        1));
        TableScan.Plan plan = read.newScan().plan();
        assertThat(plan.splits().stream().mapToLong(Split::rowCount).sum()).isEqualTo(256);
        assertThat(readIds(read, plan, Integer.MAX_VALUE).stream().min(Integer::compareTo).get())
                .isZero();
    }

    private Table createTable() throws Exception {
        try (FileSystemCatalog catalog =
                new FileSystemCatalog(LocalFileIO.create(), new Path(tempDir.toUri()))) {
            catalog.createDatabase("test_db", false);
            Identifier identifier = Identifier.create("test_db", "range_limit");
            catalog.createTable(
                    identifier,
                    Schema.newBuilder()
                            .column("id", DataTypes.INT())
                            .option("bucket", "-1")
                            .option("file.format", "parquet")
                            .option("write-only", "true")
                            .option("data-evolution.enabled", "true")
                            .option("row-tracking.enabled", "true")
                            .option("deletion-vectors.enabled", "false")
                            .build(),
                    false);
            return catalog.getTable(identifier);
        }
    }

    private void writeFile(Table table, int firstId) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (int id = firstId; id < firstId + 256; id++) {
                write.write(GenericRow.of(id));
            }
            commit.commit(write.prepareCommit());
        }
    }

    private List<Integer> readIds(ReadBuilder read, int limit) throws Exception {
        return readIds(read, read.newScan().plan(), limit);
    }

    private List<Integer> readIds(ReadBuilder read, TableScan.Plan plan, int limit)
            throws Exception {
        List<Integer> ids = new ArrayList<>();
        try (CloseableIterator<InternalRow> rows =
                read.newRead().executeFilter().createReader(plan).toCloseableIterator()) {
            // File pruning retains whole files; the consuming engine applies the global limit.
            while (ids.size() < limit && rows.hasNext()) {
                ids.add(rows.next().getInt(0));
            }
        }
        return ids;
    }
}
