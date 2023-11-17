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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CommittableTypeInfo;
import org.apache.paimon.flink.sink.StoreSinkWriteImpl;
import org.apache.paimon.flink.utils.MetricUtils;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.TraceableFileIO;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.JavaSerializer;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CdcRecordStoreWriteOperator}. */
public class CdcDynamicBucketWriteOperatorTest {

    @TempDir java.nio.file.Path tempDir;

    private Path tablePath;
    private String commitUser;

    @BeforeEach
    public void before() {
        tablePath = new Path(TraceableFileIO.SCHEME + "://" + tempDir.toString());
        commitUser = UUID.randomUUID().toString();
    }

    @AfterEach
    public void after() {
        // assert all connections are closed
        Predicate<Path> pathPredicate = path -> path.toString().contains(tempDir.toString());
        assertThat(TraceableFileIO.openInputStreams(pathPredicate)).isEmpty();
        assertThat(TraceableFileIO.openOutputStreams(pathPredicate)).isEmpty();
    }

    @Test
    public void testCompactionMetrics() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()},
                        new String[] {"pk", "col1"});
        FileStoreTable table =
                createFileStoreTable(
                        rowType, Collections.emptyList(), Collections.singletonList("pk"));
        OneInputStreamOperatorTestHarness<Tuple2<CdcRecord, Integer>, Committable> harness =
                createTestHarness(table);
        CdcDynamicBucketWriteOperator operator =
                (CdcDynamicBucketWriteOperator) harness.getOneInputOperator();
        harness.open();

        MetricGroup compactionMetricGroup =
                operator.getMetricGroup()
                        .addGroup("paimon")
                        .addGroup("table", table.name())
                        .addGroup("partition", "_")
                        .addGroup("bucket", "0")
                        .addGroup("compaction");

        long timestamp = 0;
        Map<String, String> fields = new HashMap<>();
        fields.put("pk", "1");
        fields.put("col1", "2");
        harness.processElement(Tuple2.of(new CdcRecord(RowKind.INSERT, fields), 0), timestamp++);
        operator.getWrite().compact(BinaryRow.EMPTY_ROW, 0, true);
        assertThat(
                        MetricUtils.getGauge(compactionMetricGroup, "lastTableFilesCompactedBefore")
                                .getValue())
                .isEqualTo(0L);
        assertThat(
                        MetricUtils.getGauge(compactionMetricGroup, "lastTableFilesCompactedAfter")
                                .getValue())
                .isEqualTo(1L);
        assertThat(
                        MetricUtils.getGauge(compactionMetricGroup, "lastChangelogFilesCompacted")
                                .getValue())
                .isEqualTo(0L);

        fields.put("pk", "1");
        fields.put("col1", "3");
        harness.processElement(Tuple2.of(new CdcRecord(RowKind.INSERT, fields), 0), timestamp);
        operator.getWrite().compact(BinaryRow.EMPTY_ROW, 0, true);
        assertThat(
                        MetricUtils.getGauge(compactionMetricGroup, "lastTableFilesCompactedBefore")
                                .getValue())
                .isEqualTo(1L);
        assertThat(
                        MetricUtils.getGauge(compactionMetricGroup, "lastTableFilesCompactedAfter")
                                .getValue())
                .isEqualTo(1L);
        assertThat(
                        MetricUtils.getGauge(compactionMetricGroup, "lastChangelogFilesCompacted")
                                .getValue())
                .isEqualTo(0L);

        // operator closed, metric groups should be unregistered
        harness.close();
        assertThat(MetricUtils.getGauge(compactionMetricGroup, "lastTableFilesCompactedBefore"))
                .isNull();
        assertThat(MetricUtils.getGauge(compactionMetricGroup, "lastTableFilesCompactedAfter"))
                .isNull();
        assertThat(MetricUtils.getGauge(compactionMetricGroup, "lastChangelogFilesCompacted"))
                .isNull();
    }

    private OneInputStreamOperatorTestHarness<Tuple2<CdcRecord, Integer>, Committable>
            createTestHarness(FileStoreTable table) throws Exception {
        CdcDynamicBucketWriteOperator operator =
                new CdcDynamicBucketWriteOperator(
                        table,
                        (t, commitUser, state, ioManager, memoryPool, metricGroup) ->
                                new StoreSinkWriteImpl(
                                        t,
                                        commitUser,
                                        state,
                                        ioManager,
                                        false,
                                        false,
                                        true,
                                        memoryPool,
                                        metricGroup),
                        commitUser);
        TypeSerializer<Tuple2<CdcRecord, Integer>> inputSerializer = new JavaSerializer<>();
        TypeSerializer<Committable> outputSerializer =
                new CommittableTypeInfo().createSerializer(new ExecutionConfig());
        OneInputStreamOperatorTestHarness<Tuple2<CdcRecord, Integer>, Committable> harness =
                new OneInputStreamOperatorTestHarness<>(operator, inputSerializer);
        harness.setup(outputSerializer);
        return harness;
    }

    private FileStoreTable createFileStoreTable(
            RowType rowType, List<String> partitions, List<String> primaryKeys) throws Exception {
        Options conf = new Options();
        conf.set(CdcRecordStoreWriteOperator.RETRY_SLEEP_TIME, Duration.ofMillis(10));

        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(LocalFileIO.create(), tablePath),
                        new Schema(rowType.getFields(), partitions, primaryKeys, conf.toMap(), ""));
        return FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);
    }
}
