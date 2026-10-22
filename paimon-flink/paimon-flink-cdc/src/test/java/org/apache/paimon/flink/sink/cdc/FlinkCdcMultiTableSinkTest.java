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

    @Test
    public void testDatabaseBuilderPropagatesWriterManagedMemory(
            @TempDir java.nio.file.Path tempDir) {
        Options catalogOptions = new Options();
        catalogOptions.setString("warehouse", tempDir.toString());
        Options options = new Options();
        options.setString("sink.writer-memory", "512 mb");
        options.setString("sink.writer-managed-memory", "128 mb");
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
        ResourceProfile committerResources =
                graph.getSlotSharingGroupResource("Multiplex Global Committer").get();
        assertThat(committerResources.getManagedMemory().getBytes()).isZero();
        assertThat(committerResources.getTaskHeapMemory().getMebiBytes()).isEqualTo(64);
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
}
