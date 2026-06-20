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

package org.apache.paimon.metastore;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexBuilder;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.RowRangeIndex;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link VisibilityWaitCallback}. */
public class VisibilityWaitCallbackTest extends TableTestBase {

    @Override
    protected Schema schemaDefault() {
        return Schema.newBuilder()
                .column("f0", DataTypes.INT())
                .column("f1", DataTypes.STRING())
                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                .option(CoreOptions.VISIBILITY_CALLBACK_ENABLED.key(), "true")
                .option(CoreOptions.VISIBILITY_CALLBACK_CHECK_INTERVAL.key(), "100 ms")
                .option(CoreOptions.VISIBILITY_CALLBACK_TIMEOUT.key(), "30 s")
                .build();
    }

    @Test
    public void testWaitForGlobalIndexBuildOfNewData() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeRows(table, 0, 3);
        buildIndex(table, false);

        long indexedSnapshotId = table.snapshotManager().latestSnapshot().id();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CompletableFuture<Void> writeFuture =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                writeRows(getTableDefault(), 3, 2);
                            } catch (Exception e) {
                                throw new CompletionException(e);
                            }
                        },
                        executor);

        try {
            waitUntil(() -> table.snapshotManager().latestSnapshot().id() > indexedSnapshotId);
            Thread.sleep(300L);
            assertThat(writeFuture.isDone()).isFalse();

            buildIndex(getTableDefault(), true);
            writeFuture.get(10, TimeUnit.SECONDS);

            SortedGlobalIndexBuilder builder =
                    new SortedGlobalIndexBuilder(getTableDefault(), "btree").withIndexField("f1");
            assertThat(builder.incrementalScan()).isNotPresent();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testDoNotWaitForGlobalIndexBuildOfUnindexedPartition() throws Exception {
        Identifier identifier = identifier("PartitionedTable");
        catalog.createTable(identifier, partitionedSchema(), false);
        FileStoreTable table = getTable(identifier);

        writePartitionRows(table, "a", 0, 3);
        buildPartitionIndex(table, "a");

        writePartitionRows(getTable(identifier), "b", 3, 2);
    }

    private Schema partitionedSchema() {
        return Schema.newBuilder()
                .column("pt", DataTypes.STRING())
                .column("f0", DataTypes.INT())
                .column("f1", DataTypes.STRING())
                .partitionKeys("pt")
                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                .option(CoreOptions.VISIBILITY_CALLBACK_ENABLED.key(), "true")
                .option(CoreOptions.VISIBILITY_CALLBACK_CHECK_INTERVAL.key(), "100 ms")
                .option(CoreOptions.VISIBILITY_CALLBACK_TIMEOUT.key(), "1 s")
                .build();
    }

    private void writeRows(FileStoreTable table, int start, int count) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (int i = start; i < start + count; i++) {
                write.write(GenericRow.of(i, BinaryString.fromString("a" + i)));
            }
            commit.commit(write.prepareCommit());
        }
    }

    private void writePartitionRows(FileStoreTable table, String partition, int start, int count)
            throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (int i = start; i < start + count; i++) {
                write.write(
                        GenericRow.of(
                                BinaryString.fromString(partition),
                                i,
                                BinaryString.fromString("a" + i)));
            }
            commit.commit(write.prepareCommit());
        }
    }

    private void buildIndex(FileStoreTable table, boolean incremental) throws Exception {
        SortedGlobalIndexBuilder builder =
                new SortedGlobalIndexBuilder(table, "btree").withIndexField("f1");
        Optional<Pair<RowRangeIndex, List<DataSplit>>> scan =
                incremental ? builder.incrementalScan() : builder.scan();
        assertThat(scan).isPresent();

        List<CommitMessage> commitMessages = new ArrayList<>();
        for (DataSplit dataSplit : scan.get().getRight()) {
            commitMessages.addAll(builder.build(dataSplit, ioManager));
        }

        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(commitMessages);
        }
    }

    private void buildPartitionIndex(FileStoreTable table, String partition) throws Exception {
        SortedGlobalIndexBuilder builder =
                new SortedGlobalIndexBuilder(table, "btree")
                        .withIndexField("f1")
                        .withPartitionPredicate(partitionPredicate(table, partition));
        Optional<Pair<RowRangeIndex, List<DataSplit>>> scan = builder.scan();
        assertThat(scan).isPresent();

        List<CommitMessage> commitMessages = new ArrayList<>();
        for (DataSplit dataSplit : scan.get().getRight()) {
            commitMessages.addAll(builder.build(dataSplit, ioManager));
        }

        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(commitMessages);
        }
    }

    private PartitionPredicate partitionPredicate(FileStoreTable table, String partition) {
        RowType partType = table.rowType().project("pt");
        Predicate predicate =
                PartitionPredicate.createPartitionPredicate(
                        partType,
                        Collections.singletonMap("pt", BinaryString.fromString(partition)));
        return PartitionPredicate.fromPredicate(partType, predicate);
    }

    private void waitUntil(BooleanSupplier condition) throws Exception {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(50L);
        }
        throw new AssertionError("Condition was not met before timeout.");
    }
}
