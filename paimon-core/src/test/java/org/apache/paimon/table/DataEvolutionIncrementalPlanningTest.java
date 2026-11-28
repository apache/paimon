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
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.DataFileMetaStore;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end regressions for bounded incremental metadata planning. */
class DataEvolutionIncrementalPlanningTest {

    @TempDir java.nio.file.Path directory;

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testSmallScanBufferPreservesRows(int pages) throws Exception {
        FileStoreTable table =
                createTable(false)
                        .copy(
                                Collections.singletonMap(
                                        "sort-spill-buffer-size", pages * 64 + " kb"));
        write(table, GenericRow.of(1, 0), GenericRow.of(2, 0));
        ReadBuilder read = table.newReadBuilder();
        assertThat(readIds(read, read.newScan().plan())).containsExactly(1, 2);
        try (IOManagerImpl io = new IOManagerImpl(directory.toString());
                DataFileMetaStore store = newStore(1024 * 1024)) {
            TableScan.Plan plan = plan(read, store, io);
            assertThat(readIds(read, plan)).containsExactly(1, 2);
            // The unused fallback store is sealed and stays within its original empty budget.
            if (pages == 2) {
                assertThat(store.serializedBytes()).isEqualTo(Long.BYTES);
                assertThat(store.slice(0, 0)).isEmpty();
            } else {
                assertThat(store.serializedBytes()).isGreaterThan(Long.BYTES);
            }
        }
    }

    @Test
    void testSmallLimitFitsReplayBudget() throws Exception {
        FileStoreTable table = createTable(false);
        for (int id = 0; id < 29; id++) {
            write(table, GenericRow.of(id, 0));
        }
        ReadBuilder read = table.newReadBuilder().withLimit(1);
        TableScan.Plan classic = read.newScan().plan();
        assertThat(classic.splits()).hasSize(1);
        assertThat(readIds(read, classic)).containsExactly(0);
        try (IOManagerImpl io = new IOManagerImpl(directory.toString());
                DataFileMetaStore store = newStore(1024)) {
            TableScan.Plan plan = plan(read, store, io);
            assertThat(plan.splits()).hasSize(1);
            assertThat(readIds(read, plan)).containsExactly(0);
            assertThat(store.serializedBytes()).isLessThanOrEqualTo(1024);
        }
    }

    @Test
    void testLimitDoesNotCountRowsRemovedByDataFilter() throws Exception {
        FileStoreTable table = createTable(false);
        write(table, GenericRow.of(0, 0), GenericRow.of(100, 0));
        write(table, GenericRow.of(101, 0));
        ReadBuilder read =
                table.newReadBuilder()
                        .withFilter(new PredicateBuilder(table.rowType()).greaterOrEqual(0, 100))
                        .withLimit(2);
        try (IOManagerImpl io = new IOManagerImpl(directory.toString());
                DataFileMetaStore store = newStore(1024 * 1024)) {
            assertThat(readIds(read, plan(read, store, io))).containsExactlyInAnyOrder(100, 101);
        }
    }

    @Test
    void testLimitDoesNotCountRowsOutsideRowRanges() throws Exception {
        FileStoreTable table = createTable(false);
        write(table, GenericRow.of(0, 0), GenericRow.of(1, 0));
        write(table, GenericRow.of(2, 0), GenericRow.of(3, 0));
        ReadBuilder read =
                table.newReadBuilder()
                        .withRowRanges(Arrays.asList(new Range(0, 0), new Range(2, 2)))
                        .withLimit(2);
        try (IOManagerImpl io = new IOManagerImpl(directory.toString());
                DataFileMetaStore store = newStore(1024 * 1024)) {
            assertThat(readIds(read, plan(read, store, io))).containsExactlyInAnyOrder(0, 2);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testInterleavedPartitionsKeepSplitCoalescing(boolean deletionVectors) throws Exception {
        FileStoreTable table = createTable(deletionVectors);
        write(table, GenericRow.of(0, 0), GenericRow.of(1, 1));
        for (int id = 2; id < 26; id++) {
            write(table, GenericRow.of(id, id % 2));
        }
        ReadBuilder read = table.newReadBuilder();
        TableScan.Plan classic = read.newScan().plan();
        List<Integer> expected = readIds(read, classic);
        assertThat(expected).hasSize(26).doesNotHaveDuplicates();
        try (IOManagerImpl io = new IOManagerImpl(directory.toString());
                DataFileMetaStore store = newStore(1024 * 1024)) {
            TableScan.Plan plan = plan(read, store, io);
            assertThat(plan.splits()).hasSize(classic.splits().size());
            assertThat(readIds(read, plan)).containsExactlyInAnyOrderElementsOf(expected);
            for (org.apache.paimon.table.source.Split split : plan.splits()) {
                DataSplit data = (DataSplit) split;
                assertThat(data.dataFiles()).hasSize(13);
                assertThat(data.rawConvertible()).isTrue();
            }
        }
    }

    private FileStoreTable createTable(boolean deletionVectors) throws Exception {
        try (FileSystemCatalog catalog =
                new FileSystemCatalog(
                        LocalFileIO.create(), new Path(directory.resolve("warehouse").toUri()))) {
            catalog.createDatabase("test_db", false);
            Identifier identifier = Identifier.create("test_db", "incremental_plan");
            catalog.createTable(
                    identifier,
                    Schema.newBuilder()
                            .column("id", DataTypes.INT())
                            .column("pt", DataTypes.INT())
                            .partitionKeys("pt")
                            .option("bucket", "-1")
                            .option("file.format", "parquet")
                            .option("write-only", "true")
                            .option("data-evolution.enabled", "true")
                            .option("row-tracking.enabled", "true")
                            .option("sort-spill-buffer-size", "1 mb")
                            .option("page-size", "64 kb")
                            .option("deletion-vectors.enabled", String.valueOf(deletionVectors))
                            .build(),
                    false);
            return (FileStoreTable) catalog.getTable(identifier);
        }
    }

    private void write(FileStoreTable table, InternalRow... rows) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (InternalRow row : rows) {
                write.write(row);
            }
            commit.commit(write.prepareCommit());
        }
    }

    private DataFileMetaStore newStore(long budget) throws Exception {
        return new DataFileMetaStore(Files.createTempDirectory(directory, "metadata-"), budget);
    }

    private TableScan.Plan plan(ReadBuilder read, DataFileMetaStore store, IOManagerImpl io) {
        return read.newScan(table -> table.newSnapshotReader().withIncrementalPlanning(store, io))
                .plan();
    }

    private List<Integer> readIds(ReadBuilder read, TableScan.Plan plan) throws Exception {
        List<Integer> ids = new ArrayList<>();
        try (CloseableIterator<InternalRow> rows =
                read.newRead().executeFilter().createReader(plan).toCloseableIterator()) {
            while (rows.hasNext()) {
                ids.add(rows.next().getInt(0));
            }
        }
        return ids;
    }
}
