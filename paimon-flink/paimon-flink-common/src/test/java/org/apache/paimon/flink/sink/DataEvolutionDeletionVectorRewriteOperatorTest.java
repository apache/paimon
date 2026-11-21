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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.append.dataevolution.DataEvolutionDeletionVectorRewritePlanner;
import org.apache.paimon.append.dataevolution.DataEvolutionDeletionVectorRewriteTask;
import org.apache.paimon.append.dataevolution.DataEvolutionNormalCompactTask;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.deletionvectors.DeletionVectorsIndexFile;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.paimon.table.BucketMode.UNAWARE_BUCKET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests attempt-scoped cleanup of distributed deletion-vector rewrites. */
public class DataEvolutionDeletionVectorRewriteOperatorTest extends TableTestBase {

    private static final AtomicInteger LAST_ATTEMPT = new AtomicInteger();

    private FileStoreTable table;
    private DeletionVectorsIndexFile index;
    private List<IndexFileMeta> oldIndexes;
    private List<byte[]> tasks;

    @BeforeEach
    public void prepareRewrites() throws Exception {
        createTableDefault();
        table = getTableDefault();
        try (TableCommitImpl commit = table.newCommit("insert")) {
            commit.commit(writeDataDefault(2, 4));
        }
        List<DataFileMeta> files =
                table.store().newScan().plan().files().stream()
                        .map(entry -> entry.file())
                        .sorted(Comparator.comparingLong(DataFileMeta::nonNullFirstRowId))
                        .collect(Collectors.toList());
        assertThat(files).hasSize(4);
        index = table.store().newIndexFileHandler().dvIndex(BinaryRow.EMPTY_ROW, UNAWARE_BUCKET);
        oldIndexes = new ArrayList<>();
        for (int file : new int[] {0, 2}) {
            BitmapDeletionVector bitmap = new BitmapDeletionVector();
            bitmap.delete(0);
            List<IndexFileMeta> added =
                    index.writeWithRolling(
                            Collections.singletonMap(files.get(file).fileName(), bitmap));
            oldIndexes.addAll(added);
            try (TableCommitImpl commit = table.newCommit("delete-" + file)) {
                commit.commit(
                        Collections.singletonList(
                                new CommitMessageImpl(
                                        BinaryRow.EMPTY_ROW,
                                        UNAWARE_BUCKET,
                                        null,
                                        DataIncrement.indexIncrement(added),
                                        CompactIncrement.emptyIncrement())));
            }
        }
        Snapshot snapshot = table.latestSnapshot().get();
        List<CommitMessage> compact = new ArrayList<>();
        for (int first : new int[] {0, 2}) {
            compact.add(
                    new DataEvolutionNormalCompactTask(
                                    BinaryRow.EMPTY_ROW,
                                    files.subList(first, first + 2),
                                    Collections.emptyList(),
                                    false)
                            .doCompact(table, "compact"));
        }
        tasks = new ArrayList<>();
        DataEvolutionDeletionVectorRewriteTask.Serializer serializer =
                new DataEvolutionDeletionVectorRewriteTask.Serializer();
        for (DataEvolutionDeletionVectorRewriteTask task :
                new DataEvolutionDeletionVectorRewritePlanner(
                                table.copy(
                                        Collections.singletonMap(
                                                CoreOptions.DELETION_VECTOR_INDEX_FILE_TARGET_SIZE
                                                        .key(),
                                                "1 b")),
                                snapshot)
                        .plan(compact)) {
            tasks.add(serializer.serialize(task));
        }
        assertThat(tasks).hasSizeGreaterThan(1);
    }

    @ParameterizedTest
    @ValueSource(strings = {"failure", "cancel", "success"})
    public void testAttemptCleanup(String outcome) throws Exception {
        Set<Path> before = indexFiles();
        try (OneInputStreamOperatorTestHarness<byte[], Committable> harness =
                new OneInputStreamOperatorTestHarness<>(
                        new DataEvolutionDeletionVectorRewriteOperator.Factory(table))) {
            harness.setup(new CommittableTypeInfo().createSerializer(harness.getExecutionConfig()));
            harness.open();
            harness.processElement(new StreamRecord<>(tasks.get(0)));
            assertThat(indexFiles()).hasSizeGreaterThan(before.size());
            if ("failure".equals(outcome)) {
                assertThatThrownBy(() -> harness.processElement(new StreamRecord<>(new byte[0])))
                        .isInstanceOf(Exception.class);
            } else if ("success".equals(outcome)) {
                harness.getOperator().finish();
            } else {
                // Harness.close() finishes normally; cancellation closes without finishing.
                harness.getOperator().close();
            }
        }
        if ("success".equals(outcome)) {
            assertThat(indexFiles()).hasSizeGreaterThan(before.size());
        } else {
            assertThat(indexFiles()).isEqualTo(before);
        }
    }

    @Test
    public void testFailedWorkerAttemptCleansEarlierOutputsOnRestart() throws Exception {
        index.delete(oldIndexes.get(1));
        Set<Path> before = indexFiles();
        long snapshot = table.latestSnapshot().get().id();
        LAST_ATTEMPT.set(0);
        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ZERO);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);
        env.fromCollection(tasks, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO)
                .map(
                        new RichMapFunction<byte[], byte[]>() {
                            @Override
                            public byte[] map(byte[] value) {
                                LAST_ATTEMPT.set(
                                        getRuntimeContext().getTaskInfo().getAttemptNumber());
                                return value;
                            }
                        })
                .transform(
                        "DV rewrite",
                        new CommittableTypeInfo(),
                        new DataEvolutionDeletionVectorRewriteOperator.Factory(table))
                .sinkTo(new DiscardingSink<>());
        assertThatThrownBy(env::execute).hasStackTraceContaining("Unable to read deletion vector");
        assertThat(LAST_ATTEMPT.get()).isEqualTo(1);
        assertThat(indexFiles()).isEqualTo(before);
        assertThat(table.latestSnapshot().get().id()).isEqualTo(snapshot);
    }

    private Set<Path> indexFiles() throws Exception {
        return Arrays.stream(table.fileIO().listStatus(index.path(oldIndexes.get(0)).getParent()))
                .map(status -> status.getPath())
                .collect(Collectors.toSet());
    }

    @Override
    protected Schema schemaDefault() {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .option(CoreOptions.BUCKET.key(), "-1")
                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true")
                .option(CoreOptions.WRITE_ONLY.key(), "true")
                .build();
    }

    @Override
    protected InternalRow dataDefault(int time, int size) {
        return GenericRow.of(time * size);
    }
}
