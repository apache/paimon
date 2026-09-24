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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.source.operator.MonitorSource;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.IncrementalSplit;
import org.apache.paimon.table.source.QueryAuthSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;

import static org.apache.paimon.flink.LogicalTypeConversion.toLogicalType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Test for {@link FlinkSourceBuilder}. */
public class FlinkSourceBuilderTest {

    @TempDir Path tempDir;
    private Catalog catalog;

    @BeforeEach
    public void setUp() {
        try {
            initCatalog();
        } catch (Exception e) {
            throw new RuntimeException("Catalog initialization failed", e);
        }
    }

    private void initCatalog() throws Exception {
        if (catalog == null) {
            catalog =
                    CatalogFactory.createCatalog(
                            CatalogContext.create(new org.apache.paimon.fs.Path(tempDir.toUri())));
            catalog.createDatabase("default", false);
        }
    }

    private Table createTable(
            String tableName, boolean hasPrimaryKey, int bucketNum, boolean bucketAppendOrdered)
            throws Exception {
        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .option("bucket", bucketNum + "")
                        .option("bucket-append-ordered", String.valueOf(bucketAppendOrdered));

        if (hasPrimaryKey) {
            schemaBuilder.primaryKey("a");
        }

        if (bucketNum != -1) {
            schemaBuilder.option("bucket-key", "a");
        }

        Schema schema = schemaBuilder.build();
        Identifier identifier = Identifier.create("default", tableName);
        catalog.createTable(identifier, schema, false);
        return catalog.getTable(identifier);
    }

    @Test
    public void testSplitFileSizeOrRowCountUsesDataSplitFileSize() {
        FileStoreSourceSplit split =
                new FileStoreSourceSplit(
                        "split-1",
                        DataSplit.builder()
                                .withSnapshot(1L)
                                .withPartition(org.apache.paimon.data.BinaryRow.EMPTY_ROW)
                                .withBucket(0)
                                .withBucketPath("bucket-0")
                                .withDataFiles(
                                        Arrays.asList(
                                                dataFile("file-1", 10L, 1L),
                                                dataFile("file-2", 25L, 1000L)))
                                .build());

        assertThat(SplitWeightUtils.splitFileSizeOrRowCount(split)).isEqualTo(35L);
    }

    @Test
    public void testSplitFileSizeOrRowCountUnwrapsQueryAuthSplit() {
        DataSplit dataSplit =
                DataSplit.builder()
                        .withSnapshot(1L)
                        .withPartition(org.apache.paimon.data.BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withDataFiles(
                                Arrays.asList(
                                        dataFile("file-1", 10L, 1L),
                                        dataFile("file-2", 25L, 1000L)))
                        .build();
        FileStoreSourceSplit split =
                new FileStoreSourceSplit("split-1", new QueryAuthSplit(dataSplit, null));

        assertThat(SplitWeightUtils.splitFileSizeOrRowCount(split)).isEqualTo(35L);
    }

    @Test
    public void testSplitFileSizeOrRowCountFallsBackToRowCount() {
        FileStoreSourceSplit split = new FileStoreSourceSplit("split-1", new TestSplit(123L));

        assertThat(SplitWeightUtils.splitFileSizeOrRowCount(split)).isEqualTo(123L);
    }

    private static DataFileMeta dataFile(String fileName, long fileSize, long rowCount) {
        return DataFileMeta.forAppend(
                fileName,
                fileSize,
                rowCount,
                null,
                0L,
                0L,
                0L,
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                null,
                null);
    }

    private static class TestSplit implements Split {

        private final long rowCount;

        private TestSplit(long rowCount) {
            this.rowCount = rowCount;
        }

        @Override
        public long rowCount() {
            return rowCount;
        }

        @Override
        public OptionalLong mergedRowCount() {
            return OptionalLong.of(rowCount);
        }
    }

    @Test
    public void testFileSizeWeightModeOnlyWorksWithFairAssignMode() throws Exception {
        Table table = createTable("file_size_preemptive", false, 2, false);
        Map<String, String> options = new HashMap<>();
        options.put(
                FlinkConnectorOptions.SCAN_SPLIT_ENUMERATOR_WEIGHT_MODE.key(),
                FlinkConnectorOptions.SplitWeightMode.FILE_SIZE.toString());
        options.put(
                FlinkConnectorOptions.SCAN_SPLIT_ENUMERATOR_ASSIGN_MODE.key(),
                FlinkConnectorOptions.SplitAssignMode.PREEMPTIVE.toString());
        table = table.copy(options);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkSourceBuilder builder = new FlinkSourceBuilder(table).env(env).sourceBounded(true);

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(FlinkConnectorOptions.SCAN_SPLIT_ENUMERATOR_WEIGHT_MODE.key())
                .hasMessageContaining(
                        FlinkConnectorOptions.SCAN_SPLIT_ENUMERATOR_ASSIGN_MODE.key());
    }

    @Test
    public void testUnawareBucket() throws Exception {
        // pk table && bucket-append-ordered is true
        Table table = createTable("t1", true, 2, true);
        FlinkSourceBuilder builder = new FlinkSourceBuilder(table);
        assertFalse(builder.isUnordered());

        // pk table && bucket-append-ordered is false
        table = createTable("t2", true, 2, false);
        builder = new FlinkSourceBuilder(table);
        assertFalse(builder.isUnordered());

        // pk table && bucket num == -1 && bucket-append-ordered is false
        table = createTable("t3", true, -1, false);
        builder = new FlinkSourceBuilder(table);
        assertFalse(builder.isUnordered());

        // append table && bucket num != 1 && bucket-append-ordered is true
        table = createTable("t4", false, 2, true);
        builder = new FlinkSourceBuilder(table);
        assertFalse(builder.isUnordered());

        // append table && bucket num == -1
        table = createTable("t5", false, -1, true);
        builder = new FlinkSourceBuilder(table);
        assertTrue(builder.isUnordered());

        // append table && bucket-append-ordered is false
        table = createTable("t6", false, 2, false);
        builder = new FlinkSourceBuilder(table);
        assertTrue(builder.isUnordered());
    }

    @Test
    public void testBuildWrapsStaticSourceWithPaimonDataStreamSource() throws Exception {
        Table table = createTable("static_source", false, -1, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<RowData> dataStream =
                new FlinkSourceBuilder(table).env(env).sourceBounded(true).build();

        assertThat(dataStream.getTransformation()).isInstanceOf(SourceTransformation.class);
        SourceTransformation<?, ?, ?> transformation =
                (SourceTransformation<?, ?, ?>) dataStream.getTransformation();
        assertThat(transformation.getSource()).isInstanceOf(PaimonDataStreamSource.class);
    }

    @Test
    public void testLongLimitForwardedToReadBuilder() {
        Table table = mock(Table.class);
        when(table.name()).thenReturn("table");
        when(table.options())
                .thenReturn(Collections.singletonMap("path", tempDir.toUri().toString()));
        when(table.primaryKeys()).thenReturn(Collections.emptyList());
        when(table.rowType()).thenReturn(RowType.of(DataTypes.INT()));
        ReadBuilder readBuilder = mock(ReadBuilder.class, RETURNS_SELF);
        when(table.newReadBuilder()).thenReturn(readBuilder);

        new FlinkSourceBuilder(table)
                .env(StreamExecutionEnvironment.getExecutionEnvironment())
                .sourceBounded(true)
                .limit(4294967297L)
                .build();

        verify(readBuilder).withLimit(4294967297L);
    }

    @Test
    public void testBuildWrapsContinuousSourceWithPaimonDataStreamSource() throws Exception {
        Table table = createTable("continuous_source", false, -1, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<RowData> dataStream =
                new FlinkSourceBuilder(table).env(env).sourceBounded(false).build();

        assertThat(dataStream.getTransformation()).isInstanceOf(SourceTransformation.class);
        SourceTransformation<?, ?, ?> transformation =
                (SourceTransformation<?, ?, ?>) dataStream.getTransformation();
        assertThat(transformation.getSource()).isInstanceOf(PaimonDataStreamSource.class);
    }

    @Test
    public void testPostponeMergeOnReadRejectsContinuousSource() throws Exception {
        Identifier identifier = Identifier.create("default", "postpone_merge_on_read");
        catalog.createTable(
                identifier,
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .primaryKey("a")
                        .option("bucket", "-2")
                        .option("postpone.merge-on-read", "true")
                        .build(),
                false);
        Table table = catalog.getTable(identifier);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        assertThatThrownBy(
                        () -> new FlinkSourceBuilder(table).env(env).sourceBounded(false).build())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("only supported for batch reads");
    }

    @Test
    public void testMonitorSourceBuildSourceWrapsWithPaimonDataStreamSource() throws Exception {
        Table table = createTable("monitor_source", false, -1, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<RowData> dataStream =
                MonitorSource.buildSource(
                        env,
                        "source",
                        InternalTypeInfo.of(toLogicalType(table.rowType())),
                        table.newReadBuilder(),
                        10,
                        false,
                        false,
                        false,
                        null,
                        true,
                        null,
                        table);

        assertThat(dataStream.getTransformation().getTransitivePredecessors())
                .filteredOn(Transformation.class::isInstance)
                .filteredOn(transformation -> transformation instanceof SourceTransformation)
                .anySatisfy(
                        transformation ->
                                assertThat(
                                                ((SourceTransformation<?, ?, ?>) transformation)
                                                        .getSource())
                                        .isInstanceOf(PaimonDataStreamSource.class));
    }

    @ValueSource(booleans = {false, true})
    @ParameterizedTest
    public void testOrderedShuffleRoutesQueryAuthSplitLikeTheSplitItWraps(
            boolean shuffleBucketWithPartition) throws Exception {
        Table table = createTable("ordered_shuffle_" + shuffleBucketWithPartition, false, 2, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<RowData> dataStream =
                MonitorSource.buildSource(
                        env,
                        "source",
                        InternalTypeInfo.of(toLogicalType(table.rowType())),
                        table.newReadBuilder(),
                        10,
                        false,
                        shuffleBucketWithPartition,
                        false,
                        null,
                        true,
                        null);

        Transformation<?> input = dataStream.getTransformation().getInputs().get(0);
        assertThat(input).isInstanceOf(PartitionTransformation.class);
        @SuppressWarnings("unchecked")
        StreamPartitioner<Split> partitioner =
                ((PartitionTransformation<Split>) input).getPartitioner();
        partitioner.setup(4);

        DataSplit bucketOne = dataSplit(1);
        DataSplit bucketThree = dataSplit(3);

        assertThat(selectChannel(partitioner, withQueryAuth(bucketOne)))
                .isEqualTo(selectChannel(partitioner, bucketOne));
        assertThat(selectChannel(partitioner, withQueryAuth(bucketThree)))
                .isEqualTo(selectChannel(partitioner, bucketThree));

        assertThat(selectChannel(partitioner, withQueryAuth(bucketOne)))
                .isNotEqualTo(selectChannel(partitioner, withQueryAuth(bucketThree)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testOrderedShuffleRoutesIncrementalSplitByPartitionAndBucket(
            boolean shuffleBucketWithPartition) throws Exception {
        Table table =
                createTable("ordered_incremental_" + shuffleBucketWithPartition, false, 2, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<RowData> dataStream =
                MonitorSource.buildSource(
                        env,
                        "source",
                        InternalTypeInfo.of(toLogicalType(table.rowType())),
                        table.newReadBuilder(),
                        10,
                        false,
                        shuffleBucketWithPartition,
                        false,
                        null,
                        true,
                        null);

        Transformation<?> input = dataStream.getTransformation().getInputs().get(0);
        @SuppressWarnings("unchecked")
        StreamPartitioner<Split> partitioner =
                ((PartitionTransformation<Split>) input).getPartitioner();
        partitioner.setup(4);

        Split bucketOne = incrementalSplit(1);
        Split bucketThree = incrementalSplit(3);

        assertThat(selectChannel(partitioner, bucketOne))
                .isEqualTo(selectChannel(partitioner, dataSplit(1)));
        assertThat(selectChannel(partitioner, withQueryAuth(bucketOne)))
                .isEqualTo(selectChannel(partitioner, bucketOne));
        assertThat(selectChannel(partitioner, withQueryAuth(bucketThree)))
                .isEqualTo(selectChannel(partitioner, bucketThree));

        assertThat(selectChannel(partitioner, withQueryAuth(bucketOne)))
                .isNotEqualTo(selectChannel(partitioner, withQueryAuth(bucketThree)));
    }

    @ValueSource(booleans = {false, true})
    @ParameterizedTest
    public void testOrderedShuffleRoutesFallbackWrappedQueryAuthSplitLikeTheSplitItWraps(
            boolean shuffleBucketWithPartition) throws Exception {
        Table table = createTable("ordered_nested_" + shuffleBucketWithPartition, false, 2, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<RowData> dataStream =
                MonitorSource.buildSource(
                        env,
                        "source",
                        InternalTypeInfo.of(toLogicalType(table.rowType())),
                        table.newReadBuilder(),
                        10,
                        false,
                        shuffleBucketWithPartition,
                        false,
                        null,
                        true,
                        null);

        Transformation<?> input = dataStream.getTransformation().getInputs().get(0);
        assertThat(input).isInstanceOf(PartitionTransformation.class);
        @SuppressWarnings("unchecked")
        StreamPartitioner<Split> partitioner =
                ((PartitionTransformation<Split>) input).getPartitioner();
        partitioner.setup(4);

        DataSplit bucketOne = dataSplit(1);
        DataSplit bucketThree = dataSplit(3);
        Split nestedOne = withFallback(withQueryAuth(bucketOne));
        Split nestedThree = withFallback(withQueryAuth(bucketThree));

        assertThat(nestedOne).isNotInstanceOf(DataSplit.class);
        assertThat(selectChannel(partitioner, nestedOne))
                .isEqualTo(selectChannel(partitioner, bucketOne));
        assertThat(selectChannel(partitioner, nestedThree))
                .isEqualTo(selectChannel(partitioner, bucketThree));
        assertThat(selectChannel(partitioner, nestedOne))
                .isNotEqualTo(selectChannel(partitioner, nestedThree));

        Split nestedIncrementalOne = withFallback(withQueryAuth(incrementalSplit(1)));
        assertThat(selectChannel(partitioner, nestedIncrementalOne))
                .isEqualTo(selectChannel(partitioner, bucketOne));
    }

    @ValueSource(booleans = {false, true})
    @ParameterizedTest
    public void testOrderedShuffleRoutesFallbackDataSplitLikeTheSplitItCopies(
            boolean shuffleBucketWithPartition) throws Exception {
        Table table = createTable("ordered_fallback_" + shuffleBucketWithPartition, false, 2, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<RowData> dataStream =
                MonitorSource.buildSource(
                        env,
                        "source",
                        InternalTypeInfo.of(toLogicalType(table.rowType())),
                        table.newReadBuilder(),
                        10,
                        false,
                        shuffleBucketWithPartition,
                        false,
                        null,
                        true,
                        null);

        Transformation<?> input = dataStream.getTransformation().getInputs().get(0);
        @SuppressWarnings("unchecked")
        StreamPartitioner<Split> partitioner =
                ((PartitionTransformation<Split>) input).getPartitioner();
        partitioner.setup(4);

        DataSplit bucketOne = dataSplit(1);
        DataSplit bucketThree = dataSplit(3);
        Split fallbackOne = withFallback(bucketOne);
        Split fallbackThree = withFallback(bucketThree);

        assertThat(fallbackOne).isInstanceOf(DataSplit.class);
        assertThat(selectChannel(partitioner, fallbackOne))
                .isEqualTo(selectChannel(partitioner, bucketOne));
        assertThat(selectChannel(partitioner, fallbackThree))
                .isEqualTo(selectChannel(partitioner, bucketThree));
        assertThat(selectChannel(partitioner, fallbackOne))
                .isNotEqualTo(selectChannel(partitioner, fallbackThree));
    }

    private static int selectChannel(StreamPartitioner<Split> partitioner, Split split) {
        SerializationDelegate<StreamRecord<Split>> delegate = new SerializationDelegate<>(null);
        delegate.setInstance(new StreamRecord<>(split));
        return partitioner.selectChannel(delegate);
    }

    private static DataSplit dataSplit(int bucket) {
        return DataSplit.builder()
                .withSnapshot(1)
                .withPartition(BinaryRow.EMPTY_ROW)
                .withBucket(bucket)
                .withDataFiles(Collections.emptyList())
                .isStreaming(true)
                .withBucketPath("/temp/xxx")
                .build();
    }

    private static Split incrementalSplit(int bucket) {
        return new IncrementalSplit(
                1L,
                BinaryRow.EMPTY_ROW,
                bucket,
                1,
                Collections.emptyList(),
                null,
                Collections.emptyList(),
                null,
                true);
    }

    private static Split withQueryAuth(Split split) {
        return new QueryAuthSplit(split, new TableQueryAuthResult(null, null));
    }

    private static Split withFallback(Split split) {
        return FallbackReadFileStoreTable.toFallbackSplit(split, true);
    }
}
