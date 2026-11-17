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

import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.action.MultiTablesSinkMode;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.sink.FlinkStreamPartitioner;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FlinkCdcMultiTableSink}. */
public class FlinkCdcMultiTableSinkTest {

    @Test
    public void testTransformationParallelismAndShuffle() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);
        int inputParallelism = ThreadLocalRandom.current().nextInt(8) + 1;
        DataStreamSource<CdcMultiplexRecord> input =
                env.fromData(CdcMultiplexRecord.class, new CdcMultiplexRecord("", "", null))
                        .setParallelism(inputParallelism);

        FlinkCdcMultiTableSink sink =
                new FlinkCdcMultiTableSink(
                        () -> FlinkCatalogFactory.createPaimonCatalog(new Options()),
                        "test_db",
                        FlinkConnectorOptions.SINK_WRITER_CPU.defaultValue(),
                        null,
                        FlinkConnectorOptions.SINK_COMMITTER_CPU.defaultValue(),
                        null,
                        UUID.randomUUID().toString(),
                        false,
                        null);
        DataStreamSink<?> dataStreamSink = sink.sinkFrom(input);

        // check the transformation graph
        Transformation<?> end = dataStreamSink.getTransformation();
        assertThat(end.getName()).isEqualTo("end");

        OneInputTransformation<?, ?> committer =
                (OneInputTransformation<?, ?>) end.getInputs().get(0);
        assertThat(committer.getName()).isEqualTo("Multiplex Global Committer");
        assertThat(committer.getParallelism()).isEqualTo(inputParallelism);

        PartitionTransformation<?> partitioner =
                (PartitionTransformation<?>) committer.getInputs().get(0);
        assertThat(partitioner.getParallelism()).isEqualTo(inputParallelism);

        OneInputTransformation<?, ?> writer =
                (OneInputTransformation<?, ?>) partitioner.getInputs().get(0);
        assertThat(writer.getName()).isEqualTo("CDC MultiplexWriter");
        assertThat(writer.getParallelism()).isEqualTo(inputParallelism);

        // The sink must shuffle its input by bucket itself, otherwise the writer states restored by
        // CdcRecordStoreMultiWriteOperator would land in subtasks which never write the
        // corresponding buckets. Do not drop this shuffle.
        PartitionTransformation<?> writerInput =
                (PartitionTransformation<?>) writer.getInputs().get(0);
        assertThat(writerInput.getPartitioner()).isInstanceOf(FlinkStreamPartitioner.class);
        assertThat(writerInput.getPartitioner()).hasToString("shuffle by bucket");
        assertThat(writerInput.getParallelism()).isEqualTo(inputParallelism);
        assertThat(writerInput.getInputs().get(0)).isSameAs(input.getTransformation());
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testCompatibilityConstructorPreservesInputTopology() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<CdcMultiplexRecord> input =
                env.fromData(CdcMultiplexRecord.class, new CdcMultiplexRecord("", "", null));

        FlinkCdcMultiTableSink sink =
                new FlinkCdcMultiTableSink(
                        () -> FlinkCatalogFactory.createPaimonCatalog(new Options()),
                        FlinkConnectorOptions.SINK_WRITER_CPU.defaultValue(),
                        null,
                        FlinkConnectorOptions.SINK_COMMITTER_CPU.defaultValue(),
                        null,
                        UUID.randomUUID().toString(),
                        false,
                        null);
        Transformation<?> end = sink.sinkFrom(input).getTransformation();
        OneInputTransformation<?, ?> committer =
                (OneInputTransformation<?, ?>) end.getInputs().get(0);
        PartitionTransformation<?> committablePartitioner =
                (PartitionTransformation<?>) committer.getInputs().get(0);
        OneInputTransformation<?, ?> writer =
                (OneInputTransformation<?, ?>) committablePartitioner.getInputs().get(0);

        assertThat(writer.getInputs().get(0)).isSameAs(input.getTransformation());
        assertThatThrownBy(() -> sink.sinkFrom(input, 4))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Explicit parallelism")
                .hasMessageContaining("database name");
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"1536 kb"})
    public void testDatabaseBuilderPropagatesWriterResources(
            String offHeapMemory, @TempDir java.nio.file.Path tempDir) {
        Options catalogOptions = new Options();
        catalogOptions.setString("warehouse", tempDir.toString());
        Options options = new Options();
        options.setString("sink.writer-memory", "512 mb");
        options.setString("sink.writer-managed-memory", "128 mb");
        if (offHeapMemory != null) {
            options.setString("sink.writer-offheap-memory", offHeapMemory);
        }
        options.setString("sink.committer-memory", "64 mb");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        new FlinkCdcSyncDatabaseSinkBuilder<TestCdcEvent>()
                .withInput(
                        env.fromData(
                                TestCdcEvent.class,
                                new TestCdcEvent("t", Collections.emptyList(), 0)))
                .withParserFactory(TestCdcEventParser::new)
                .withTableOptions(options)
                .withDatabase("test_db")
                .withCatalogLoader(() -> FlinkCatalogFactory.createPaimonCatalog(catalogOptions))
                .withTypeMapping(TypeMapping.defaultMapping())
                .withMode(MultiTablesSinkMode.COMBINED)
                .build();
        StreamGraph graph = env.getStreamGraph();
        ResourceProfile writerResources =
                graph.getSlotSharingGroupResource("CDC MultiplexWriter").get();
        assertThat(writerResources.getManagedMemory().getMebiBytes()).isEqualTo(128);
        assertThat(writerResources.getTaskHeapMemory().getMebiBytes()).isEqualTo(512);
        assertThat(writerResources.getTaskOffHeapMemory().getBytes())
                .isEqualTo(offHeapMemory == null ? 0 : 1572864);
        ResourceProfile committerResources =
                graph.getSlotSharingGroupResource("Multiplex Global Committer").get();
        assertThat(committerResources.getManagedMemory().getBytes()).isZero();
        assertThat(committerResources.getTaskHeapMemory().getMebiBytes()).isEqualTo(64);
        assertThat(committerResources.getTaskOffHeapMemory().getBytes()).isZero();
    }

    @Test
    public void testLegacyManagedMemoryConstructorPreservesResources() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        new FlinkCdcMultiTableSink(
                        () -> FlinkCatalogFactory.createPaimonCatalog(new Options()),
                        "test_db",
                        2.5,
                        MemorySize.ofMebiBytes(512),
                        MemorySize.ofMebiBytes(128),
                        1.0,
                        null,
                        "test-user",
                        false,
                        null)
                .sinkFrom(
                        env.fromData(
                                CdcMultiplexRecord.class, new CdcMultiplexRecord("", "", null)));
        ResourceProfile resources =
                env.getStreamGraph().getSlotSharingGroupResource("CDC MultiplexWriter").get();
        assertThat(resources.getTaskOffHeapMemory().getBytes()).isZero();
        assertThat(resources.getManagedMemory().getMebiBytes()).isEqualTo(128);
        assertThat(resources.getTaskHeapMemory().getMebiBytes()).isEqualTo(512);
        assertThat(resources.getCpuCores().getValue().doubleValue()).isEqualTo(2.5);
    }

    @Test
    public void testOffHeapMemoryWithoutHeapIsRejected() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkCdcMultiTableSink sink =
                new FlinkCdcMultiTableSink(
                        () -> FlinkCatalogFactory.createPaimonCatalog(new Options()),
                        "test_db",
                        1.0,
                        null,
                        null,
                        MemorySize.ofMebiBytes(128),
                        1.0,
                        null,
                        "test-user",
                        false,
                        null);
        assertThatThrownBy(
                        () ->
                                sink.sinkFrom(
                                        env.fromData(
                                                CdcMultiplexRecord.class,
                                                new CdcMultiplexRecord("", "", null))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sink.writer-offheap-memory")
                .hasMessageContaining("sink.writer-memory");
    }

    @Test
    public void testManagedMemoryWithoutHeapIsRejected() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkCdcMultiTableSink sink =
                new FlinkCdcMultiTableSink(
                        () -> FlinkCatalogFactory.createPaimonCatalog(new Options()),
                        "test_db",
                        1.0,
                        null,
                        MemorySize.ofMebiBytes(128),
                        1.0,
                        null,
                        "test-user",
                        false,
                        null);
        assertThatThrownBy(
                        () ->
                                sink.sinkFrom(
                                        env.fromData(
                                                CdcMultiplexRecord.class,
                                                new CdcMultiplexRecord("", "", null))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sink.writer-managed-memory")
                .hasMessageContaining("sink.writer-memory");
    }

    @Test
    public void testDatabaseAwareConstructorRejectsNullDatabase() {
        assertThatThrownBy(
                        () ->
                                new FlinkCdcMultiTableSink(
                                        () ->
                                                FlinkCatalogFactory.createPaimonCatalog(
                                                        new Options()),
                                        null,
                                        FlinkConnectorOptions.SINK_WRITER_CPU.defaultValue(),
                                        null,
                                        FlinkConnectorOptions.SINK_COMMITTER_CPU.defaultValue(),
                                        null,
                                        UUID.randomUUID().toString(),
                                        false,
                                        null))
                .isInstanceOf(NullPointerException.class);
    }

    @ParameterizedTest
    @CsvSource({
        "64 mb, 128 mb, 67108864, 134217728",
        "1536 kb, , 1572864, 0",
        ", 512 kb, 0, 524288"
    })
    public void testDatabaseBuilderPropagatesCommitterResources(
            String offHeap,
            String managed,
            long offHeapBytes,
            long managedBytes,
            @TempDir java.nio.file.Path tempDir) {
        Options options = new Options();
        options.setString("sink.writer-cpu", "2.5");
        options.setString("sink.writer-memory", "512 mb");
        options.setString("sink.writer-offheap-memory", "32 mb");
        options.setString("sink.writer-managed-memory", "16 mb");
        options.setString("sink.committer-cpu", "0.25");
        options.setString("sink.committer-memory", "256 mb");
        if (offHeap != null) {
            options.setString("sink.committer-offheap-memory", offHeap);
        }
        if (managed != null) {
            options.setString("sink.committer-managed-memory", managed);
        }
        StreamGraph graph = databaseGraph(options, tempDir);
        ResourceProfile writer = graph.getSlotSharingGroupResource("CDC MultiplexWriter").get();
        assertThat(writer.getCpuCores().getValue().doubleValue()).isEqualTo(2.5);
        assertThat(writer.getTaskHeapMemory().getMebiBytes()).isEqualTo(512);
        assertThat(writer.getTaskOffHeapMemory().getMebiBytes()).isEqualTo(32);
        assertThat(writer.getManagedMemory().getMebiBytes()).isEqualTo(16);
        ResourceProfile committer =
                graph.getSlotSharingGroupResource("Multiplex Global Committer").get();
        assertThat(committer.getCpuCores().getValue().doubleValue()).isEqualTo(0.25);
        assertThat(committer.getTaskHeapMemory().getMebiBytes()).isEqualTo(256);
        assertThat(committer.getTaskOffHeapMemory().getBytes()).isEqualTo(offHeapBytes);
        assertThat(committer.getManagedMemory().getBytes()).isEqualTo(managedBytes);
        assertThat(graph.getJobGraph().getVerticesSortedTopologicallyFromSources())
                .filteredOn(vertex -> vertex.getName().startsWith("Multiplex Global Committer"))
                .singleElement()
                .satisfies(
                        vertex ->
                                assertThat(vertex.getSlotSharingGroup().getResourceProfile())
                                        .isEqualTo(committer));
    }

    @ParameterizedTest
    @ValueSource(strings = {"sink.committer-offheap-memory", "sink.committer-managed-memory"})
    public void testCommitterMemoryRequiresCommitterHeap(
            String option, @TempDir java.nio.file.Path tempDir) {
        Options options = new Options();
        options.setString("sink.writer-memory", "512 mb");
        options.setString(option, "128 mb");
        assertThatThrownBy(() -> databaseGraph(options, tempDir))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(option)
                .hasMessageContaining("sink.committer-memory")
                .hasMessageNotContaining("sink.writer-");
    }

    @Test
    public void testLegacyOffHeapConstructorPreservesCommitterResources() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        new FlinkCdcMultiTableSink(
                        () -> FlinkCatalogFactory.createPaimonCatalog(new Options()),
                        "test_db",
                        2.5,
                        MemorySize.ofMebiBytes(512),
                        MemorySize.ofMebiBytes(16),
                        MemorySize.ofMebiBytes(32),
                        0.25,
                        MemorySize.ofMebiBytes(256),
                        "test-user",
                        false,
                        null)
                .sinkFrom(
                        env.fromData(
                                CdcMultiplexRecord.class, new CdcMultiplexRecord("", "", null)));
        ResourceProfile resources =
                env.getStreamGraph()
                        .getSlotSharingGroupResource("Multiplex Global Committer")
                        .get();
        assertThat(resources.getCpuCores().getValue().doubleValue()).isEqualTo(0.25);
        assertThat(resources.getTaskHeapMemory().getMebiBytes()).isEqualTo(256);
        assertThat(resources.getTaskOffHeapMemory().getBytes()).isZero();
        assertThat(resources.getManagedMemory().getBytes()).isZero();
    }

    private StreamGraph databaseGraph(Options options, java.nio.file.Path tempDir) {
        Options catalogOptions = new Options();
        catalogOptions.setString("warehouse", tempDir.toString());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        new FlinkCdcSyncDatabaseSinkBuilder<TestCdcEvent>()
                .withInput(
                        env.fromData(
                                TestCdcEvent.class,
                                new TestCdcEvent("t", Collections.emptyList(), 0)))
                .withParserFactory(TestCdcEventParser::new)
                .withTableOptions(options)
                .withDatabase("test_db")
                .withCatalogLoader(() -> FlinkCatalogFactory.createPaimonCatalog(catalogOptions))
                .withTypeMapping(TypeMapping.defaultMapping())
                .withMode(MultiTablesSinkMode.COMBINED)
                .build();
        return env.getStreamGraph();
    }
}
