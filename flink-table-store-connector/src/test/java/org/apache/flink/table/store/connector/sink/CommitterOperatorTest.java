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

package org.apache.flink.table.store.connector.sink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.file.manifest.ManifestCommittableSerializer;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.UpdateSchema;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.file.utils.RecordReaderIterator;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.FileStoreTableFactory;
import org.apache.flink.table.store.table.sink.FileCommittable;
import org.apache.flink.table.store.table.sink.TableWrite;
import org.apache.flink.table.store.table.source.TableRead;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/** Tests for {@link CommitterOperator}. */
public class CommitterOperatorTest {

    private static final RowType ROW_TYPE =
            RowType.of(
                    new LogicalType[] {
                        DataTypes.INT().getLogicalType(), DataTypes.BIGINT().getLogicalType()
                    },
                    new String[] {"a", "b"});

    @TempDir public java.nio.file.Path tempDir;
    private Path tablePath;
    private String initialCommitUser;

    @BeforeEach
    public void before() {
        tablePath = new Path(tempDir.toString());
        initialCommitUser = UUID.randomUUID().toString();
    }

    @Test
    public void testFailIntentionallyAfterRestore() throws Exception {
        FileStoreTable table = createFileStoreTable();

        OneInputStreamOperatorTestHarness<Committable, Committable> testHarness =
                createTestHarness(table);
        testHarness.open();

        TableWrite write = table.newWrite(initialCommitUser);
        write.write(GenericRowData.of(1, 10L));
        write.write(GenericRowData.of(2, 20L));

        long timestamp = 1;
        for (FileCommittable committable : write.prepareCommit(false, 8)) {
            testHarness.processElement(
                    new Committable(8, Committable.Kind.FILE, committable), timestamp++);
        }
        // checkpoint is completed but not notified, so no snapshot is committed
        OperatorSubtaskState snapshot = testHarness.snapshot(0, timestamp++);
        assertThat(table.snapshotManager().latestSnapshotId()).isNull();

        testHarness = createTestHarness(table);
        try {
            // commit snapshot from state, fail intentionally
            testHarness.initializeState(snapshot);
            testHarness.open();
            fail("Expecting intentional exception");
        } catch (Exception e) {
            assertThat(e)
                    .hasMessageContaining(
                            "This exception is intentionally thrown "
                                    + "after committing the restored checkpoints. "
                                    + "By restarting the job we hope that "
                                    + "writers can start writing based on these new commits.");
        }
        assertResults(table, "1, 10", "2, 20");

        // snapshot is successfully committed, no failure is needed
        testHarness = createTestHarness(table);
        testHarness.initializeState(snapshot);
        testHarness.open();
        assertResults(table, "1, 10", "2, 20");
    }

    @Test
    public void testCheckpointAbort() throws Exception {
        FileStoreTable table = createFileStoreTable();
        OneInputStreamOperatorTestHarness<Committable, Committable> testHarness =
                createTestHarness(table);
        testHarness.open();

        // files from multiple checkpoint
        // but no snapshot
        long cpId = 0;
        for (int i = 0; i < 10; i++) {
            cpId++;
            TableWrite write = table.newWrite(initialCommitUser);
            write.write(GenericRowData.of(1, 10L));
            write.write(GenericRowData.of(2, 20L));
            for (FileCommittable committable : write.prepareCommit(false, cpId)) {
                testHarness.processElement(
                        new Committable(cpId, Committable.Kind.FILE, committable), 1);
            }
        }

        // checkpoint is completed but not notified, so no snapshot is committed
        testHarness.snapshot(cpId, 1);
        testHarness.notifyOfCompletedCheckpoint(cpId);

        SnapshotManager snapshotManager = new SnapshotManager(tablePath);

        // should create 10 snapshots
        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(cpId);
    }

    private void assertResults(FileStoreTable table, String... expected) {
        TableRead read = table.newRead();
        List<String> actual = new ArrayList<>();
        table.newScan()
                .plan()
                .splits
                .forEach(
                        s -> {
                            try {
                                RecordReader<RowData> recordReader = read.createReader(s);
                                CloseableIterator<RowData> it =
                                        new RecordReaderIterator<>(recordReader);
                                while (it.hasNext()) {
                                    RowData row = it.next();
                                    actual.add(row.getInt(0) + ", " + row.getLong(1));
                                }
                                it.close();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
        Collections.sort(actual);
        assertThat(actual).isEqualTo(Arrays.asList(expected));
    }

    private FileStoreTable createFileStoreTable() throws Exception {
        Configuration conf = new Configuration();
        conf.set(CoreOptions.PATH, tablePath.toString());
        SchemaManager schemaManager = new SchemaManager(tablePath);
        schemaManager.commitNewVersion(
                new UpdateSchema(
                        ROW_TYPE,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        conf.toMap(),
                        ""));
        return FileStoreTableFactory.create(conf);
    }

    private OneInputStreamOperatorTestHarness<Committable, Committable> createTestHarness(
            FileStoreTable table) throws Exception {
        CommitterOperator operator =
                new CommitterOperator(
                        true,
                        initialCommitUser,
                        user -> new StoreCommitter(table.newCommit(user)),
                        ManifestCommittableSerializer::new);
        TypeSerializer<Committable> serializer =
                new CommittableTypeInfo().createSerializer(new ExecutionConfig());
        OneInputStreamOperatorTestHarness<Committable, Committable> harness =
                new OneInputStreamOperatorTestHarness<>(operator, serializer);
        harness.setup(serializer);
        return harness;
    }
}
