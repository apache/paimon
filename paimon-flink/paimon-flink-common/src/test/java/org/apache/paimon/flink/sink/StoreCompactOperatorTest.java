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

package org.apache.paimon.flink.sink;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.operation.WriteRestore;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.SinkRecord;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.PlanImpl;
import org.apache.paimon.table.source.StreamDataTableScan;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.table.system.CompactBucketsTable;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SerializationUtils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link StoreCompactOperator}. */
public class StoreCompactOperatorTest extends TableTestBase {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testCompactExactlyOnce(boolean streamingMode) throws Exception {
        createTableDefault();

        CompactRememberStoreWrite compactRememberStoreWrite =
                new CompactRememberStoreWrite(streamingMode);
        StoreCompactOperator.Factory operatorFactory =
                new StoreCompactOperator.Factory(
                        getTableDefault(),
                        (table, commitUser, state, ioManager, memoryPoolFactory, metricGroup) ->
                                compactRememberStoreWrite,
                        "10086",
                        !streamingMode);

        TypeSerializer<Committable> serializer =
                new CommittableTypeInfo().createSerializer(new ExecutionConfig());
        OneInputStreamOperatorTestHarness<RowData, Committable> harness =
                new OneInputStreamOperatorTestHarness<>(operatorFactory);
        harness.setup(serializer);
        harness.initializeEmptyState();
        harness.open();

        harness.processElement(new StreamRecord<>(data(0)));
        harness.processElement(new StreamRecord<>(data(0)));
        harness.processElement(new StreamRecord<>(data(1)));
        harness.processElement(new StreamRecord<>(data(1)));
        harness.processElement(new StreamRecord<>(data(2)));

        StoreCompactOperator operator = (StoreCompactOperator) harness.getOperator();
        assertThat(operator.compactionWaitingSet())
                .containsExactlyInAnyOrder(
                        Pair.of(BinaryRow.EMPTY_ROW, 0),
                        Pair.of(BinaryRow.EMPTY_ROW, 1),
                        Pair.of(BinaryRow.EMPTY_ROW, 2));
        assertThat(compactRememberStoreWrite.compactTime).isEqualTo(0);
        operator.prepareCommit(true, 1);
        assertThat(operator.compactionWaitingSet()).isEmpty();
        assertThat(compactRememberStoreWrite.compactTime).isEqualTo(3);
    }

    @Test
    public void testStreamingCompactConflictWithOverwrite() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .partitionKeys("pt")
                        .primaryKey("pt", "a")
                        .option("bucket", "1")
                        .build();
        Identifier identifier = identifier();
        catalog.createTable(identifier, schema, false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);

        String writeJobCommitUser = UUID.randomUUID().toString();
        String compactJobCommitUser = UUID.randomUUID().toString();

        CompactBucketsTable compactBucketsTable = new CompactBucketsTable(table, true);
        StreamDataTableScan scan = compactBucketsTable.newStreamScan();
        InnerTableRead read = compactBucketsTable.newRead();

        CheckpointConfig checkpointConfig = new CheckpointConfig();
        checkpointConfig.setCheckpointInterval(500);
        StoreCompactOperator.Factory operatorFactory =
                new StoreCompactOperator.Factory(
                        table,
                        StoreSinkWrite.createWriteProvider(
                                table, checkpointConfig, true, false, false),
                        compactJobCommitUser,
                        true);

        TypeSerializer<Committable> serializer =
                new CommittableTypeInfo().createSerializer(new ExecutionConfig());
        OneInputStreamOperatorTestHarness<RowData, Committable> harness =
                new OneInputStreamOperatorTestHarness<>(operatorFactory);
        harness.setup(serializer);
        harness.initializeEmptyState();
        harness.open();
        StoreCompactOperator operator = (StoreCompactOperator) harness.getOperator();

        FileStoreTable writeOnlyTable = table.copy(Collections.singletonMap("write-only", "true"));

        // write base data
        batchWriteAndCommit(writeOnlyTable, writeJobCommitUser, null, GenericRow.of(1, 1, 100));
        read.createReader(scan.plan())
                .forEachRemaining(
                        row -> {
                            try {
                                harness.processElement(new StreamRecord<>(new FlinkRowData(row)));
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        List<Committable> committables1 = operator.prepareCommit(true, 1);
        commit(table, compactJobCommitUser, committables1, 1);
        assertThat(table.snapshotManager().latestSnapshot().commitKind())
                .isEqualTo(Snapshot.CommitKind.COMPACT);

        // overwrite and insert
        batchWriteAndCommit(
                writeOnlyTable,
                writeJobCommitUser,
                Collections.singletonMap("pt", "1"),
                GenericRow.of(1, 2, 200));
        batchWriteAndCommit(writeOnlyTable, writeJobCommitUser, null, GenericRow.of(1, 3, 300));
        assertThat(table.snapshotManager().latestSnapshot().id()).isEqualTo(4);
        TableScan.Plan plan = scan.plan();
        assertThat(((PlanImpl) plan).snapshotId()).isEqualTo(4);
        read.createReader(plan)
                .forEachRemaining(
                        row -> {
                            try {
                                harness.processElement(new StreamRecord<>(new FlinkRowData(row)));
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        List<Committable> committables2 = operator.prepareCommit(true, 2);
        assertThatThrownBy(() -> commit(table, compactJobCommitUser, committables2, 2))
                .hasMessageContaining("File deletion conflicts detected! Give up committing.");
    }

    private RowData data(int bucket) {
        GenericRow genericRow =
                GenericRow.of(
                        0L,
                        SerializationUtils.serializeBinaryRow(BinaryRow.EMPTY_ROW),
                        bucket,
                        new byte[] {0x00, 0x00, 0x00, 0x00});
        return new FlinkRowData(genericRow);
    }

    private void batchWriteAndCommit(
            FileStoreTable table,
            String commitUser,
            @Nullable Map<String, String> overwritePartition,
            InternalRow... rows)
            throws Exception {
        try (TableWriteImpl<?> write = table.newWrite(commitUser);
                TableCommitImpl commit =
                        table.newCommit(commitUser).withOverwrite(overwritePartition)) {
            for (InternalRow row : rows) {
                write.write(row);
            }
            commit.commit(write.prepareCommit());
        }
    }

    private void commit(
            FileStoreTable table,
            String commitUser,
            List<Committable> committables,
            long checkpointId)
            throws Exception {
        try (TableCommitImpl commit = table.newCommit(commitUser)) {
            StoreCommitter committer =
                    new StoreCommitter(
                            table,
                            commit,
                            Committer.createContext(commitUser, null, true, false, null, 1, 1));
            ManifestCommittable manifestCommittable =
                    committer.combine(checkpointId, System.currentTimeMillis(), committables);
            committer.commit(Collections.singletonList(manifestCommittable));
        }
    }

    private static class CompactRememberStoreWrite implements StoreSinkWrite {

        private final boolean streamingMode;
        private int compactTime = 0;

        public CompactRememberStoreWrite(boolean streamingMode) {
            this.streamingMode = streamingMode;
        }

        @Override
        public void setWriteRestore(WriteRestore writeRestore) {}

        @Override
        public SinkRecord write(InternalRow rowData) {
            return null;
        }

        @Override
        public SinkRecord write(InternalRow rowData, int bucket) {
            return null;
        }

        @Override
        public void compact(BinaryRow partition, int bucket, boolean fullCompaction) {
            compactTime++;
        }

        @Override
        public void notifyNewFiles(
                long snapshotId, BinaryRow partition, int bucket, List<DataFileMeta> files) {}

        @Override
        public List<Committable> prepareCommit(boolean waitCompaction, long checkpointId) {
            return null;
        }

        @Override
        public void snapshotState() {}

        @Override
        public boolean streamingMode() {
            return streamingMode;
        }

        @Override
        public void close() {}

        @Override
        public void replace(FileStoreTable newTable) {}
    }
}
