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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.BlobPlaceholder;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end incremental compaction of dedicated blob and vector-store files. */
public class DataEvolutionDedicatedIncrementalCompactionTest extends TableTestBase {

    private static final int ROW_COUNT = 16;

    @ParameterizedTest
    @CsvSource({
        "blob,true,false,false",
        "json,true,false,false",
        "parquet,true,false,false",
        "blob,true,true,false",
        "json,true,true,false",
        "parquet,true,true,false",
        "blob,false,false,false",
        "json,false,false,false",
        "parquet,false,false,false",
        "parquet,true,false,true"
    })
    public void testRetainsOlderProvidersAndCompactsNewestVersions(
            String format, boolean incremental, boolean concurrentUpdate, boolean rollOutput)
            throws Exception {
        boolean blob = format.equals("blob");
        int rowCount = rollOutput ? 1100 : ROW_COUNT;
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(
                                CoreOptions.DATA_EVOLUTION_COMPACTION_INCREMENTAL_ENABLED.key(),
                                Boolean.toString(incremental))
                        .option(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "3")
                        .option(CoreOptions.FILE_COMPRESSION.key(), "none");
        if (blob) {
            schema.column("payload", DataTypes.BLOB());
        } else {
            schema.column("cold", DataTypes.VECTOR(256, DataTypes.FLOAT()))
                    .column("payload", DataTypes.VECTOR(2, DataTypes.FLOAT()))
                    .option(CoreOptions.VECTOR_FILE_FORMAT.key(), format);
        }
        catalog.createTable(identifier(), schema.build(), false);
        FileStoreTable table = getTableDefault();
        Random random = new Random(42);
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite writer = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (int i = 0; i < rowCount; i++) {
                if (blob) {
                    byte[] bytes = new byte[4096];
                    random.nextBytes(bytes);
                    writer.write(GenericRow.of(i, new BlobData(bytes)));
                } else {
                    float[] cold = new float[256];
                    for (int j = 0; j < cold.length; j++) {
                        cold[j] = random.nextFloat();
                    }
                    writer.write(
                            GenericRow.of(i, BinaryVector.fromPrimitiveArray(cold), vector(i)));
                }
            }
            commit.commit(writer.prepareCommit());
        }
        List<DataFileMeta> baseFiles = files(table);
        assertThat(baseFiles).hasSize(2);
        for (int version = 1; version <= 3; version++) {
            update(table, blob, version, rowCount);
        }
        if (rollOutput) {
            table =
                    table.copy(
                            Collections.singletonMap(
                                    CoreOptions.VECTOR_TARGET_FILE_SIZE.key(), "1 b"));
        }
        List<String> expected = readValues(table, blob);
        long historicalSnapshot = table.snapshotManager().latestSnapshotId();
        List<DataEvolutionCompactTask> tasks = plan(table, blob);
        assertThat(tasks).hasSize(1);
        DataEvolutionCompactTask task = tasks.get(0);
        assertThat(task.type())
                .isEqualTo(
                        blob
                                ? incremental
                                        ? DataEvolutionCompactTask.TaskType.INCREMENTAL_BLOB
                                        : DataEvolutionCompactTask.TaskType.BLOB
                                : DataEvolutionCompactTask.TaskType.VECTOR);
        assertThat(task.compactBefore()).hasSize(incremental ? 3 : 4);
        if (incremental) {
            assertThat(task.compactBefore()).doesNotContainAnyElementsOf(baseFiles);
        }
        DataEvolutionCompactTaskSerializer serializer = new DataEvolutionCompactTaskSerializer();
        task = serializer.deserialize(serializer.getVersion(), serializer.serialize(task));
        if (concurrentUpdate) {
            update(table, blob, 4, rowCount);
        }
        List<String> expectedAfterCommit = readValues(table, blob);
        List<CommitMessage> messages = new ArrayList<>();
        messages.add(task.doCompact(table, "dedicated-incremental"));
        messages.addAll(
                new DataEvolutionCompactionCommitPreparation(
                                table, table.snapshotManager().latestSnapshot())
                        .prepare(messages));
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(messages);
        }
        if (incremental) {
            assertThat(files(table)).containsAll(baseFiles);
        }
        assertThat(readValues(table, blob))
                .containsExactlyInAnyOrderElementsOf(expectedAfterCommit);
        assertThat(
                        readValues(
                                table.copy(
                                        Collections.singletonMap(
                                                CoreOptions.SCAN_SNAPSHOT_ID.key(),
                                                Long.toString(historicalSnapshot))),
                                blob))
                .containsExactlyInAnyOrderElementsOf(expected);
        if (!concurrentUpdate) {
            if (rollOutput) {
                assertThat(task.compactAfter()).hasSize(2);
                // Rolled outputs no longer have the same range as the retained base. The next
                // task must cover the whole group rather than select another version prefix.
                List<DataEvolutionCompactTask> nextTasks = plan(table, false);
                assertThat(nextTasks).hasSize(1);
                assertThat(nextTasks.get(0).compactBefore())
                        .hasSize(3)
                        .containsAll(task.compactAfter());
            } else {
                assertThat(plan(table, blob)).isEmpty();
            }
        }
    }

    private void update(FileStoreTable table, boolean blob, int version, int rowCount)
            throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite writer =
                        builder.newWrite()
                                .withWriteType(
                                        table.rowType()
                                                .project(Collections.singletonList("payload")));
                BatchTableCommit commit = builder.newCommit()) {
            for (int i = 0; i < rowCount; i++) {
                Object value =
                        i == 1
                                ? null
                                : blob
                                        ? i == 0
                                                ? BlobPlaceholder.INSTANCE
                                                : new BlobData(
                                                        new byte[] {(byte) version, (byte) i})
                                        : vector(version * 100 + i);
                writer.write(GenericRow.of(value));
            }
            List<CommitMessage> messages = writer.prepareCommit();
            for (CommitMessage message : messages) {
                ((CommitMessageImpl) message)
                        .newFilesIncrement()
                        .newFiles()
                        .replaceAll(file -> file.assignFirstRowId(0L));
            }
            commit.commit(messages);
        }
    }

    private List<DataEvolutionCompactTask> plan(FileStoreTable table, boolean blob) {
        return new DataEvolutionCompactCoordinator(
                        table, blob, !blob, table.snapshotManager().latestSnapshot())
                .plan();
    }

    private List<DataFileMeta> files(FileStoreTable table) {
        return table.store().newScan().plan().files().stream()
                .map(ManifestEntry::file)
                .collect(Collectors.toList());
    }

    private List<String> readValues(FileStoreTable table, boolean blob) throws Exception {
        List<String> values = new ArrayList<>();
        ReadBuilder builder = table.newReadBuilder();
        try (RecordReader<InternalRow> reader =
                builder.newRead().createReader(builder.newScan().plan())) {
            reader.forEachRemaining(
                    row -> {
                        int payloadIndex = blob ? 1 : 2;
                        values.add(
                                row.getInt(0)
                                        + "|"
                                        + (blob
                                                ? ""
                                                : Arrays.toString(row.getVector(1).toFloatArray()))
                                        + "|"
                                        + (row.isNullAt(payloadIndex)
                                                ? "null"
                                                : blob
                                                        ? Arrays.toString(
                                                                row.getBlob(payloadIndex).toData())
                                                        : Arrays.toString(
                                                                row.getVector(payloadIndex)
                                                                        .toFloatArray())));
                    });
        }
        return values;
    }

    private static BinaryVector vector(float value) {
        return BinaryVector.fromPrimitiveArray(new float[] {value, 0});
    }
}
