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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the writer slot sharing group's managed memory budget. */
class WriterManagedMemoryTest extends CommitterTestBase {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testManagedResourcesAndDownstreamInheritance(boolean managedAllocator) throws Exception {
        FileStoreTable table = createTable("128 mb", managedAllocator);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Committable> written = write(env, table);
        StreamGraph graph = env.getStreamGraph();
        String writerGroup = written.getTransformation().getName();
        ResourceProfile resources = graph.getSlotSharingGroupResource(writerGroup).get();
        assertThat(resources.getManagedMemory()).isEqualTo(MemorySize.ofMebiBytes(128));
        assertThat(resources.getTaskHeapMemory()).isEqualTo(MemorySize.ofMebiBytes(512));
        assertThat(resources.getCpuCores().getValue().doubleValue()).isEqualTo(2.5);
        assertThat(resources.getTaskOffHeapMemory()).isEqualTo(MemorySize.ZERO);
        assertThat(written.getTransformation().getManagedMemoryOperatorScopeUseCaseWeights())
                .isEqualTo(
                        managedAllocator
                                ? Collections.singletonMap(ManagedMemoryUseCase.OPERATOR, 256)
                                : Collections.emptyMap());
        graph.getStreamNodes().stream()
                .filter(n -> !n.getOperatorName().startsWith("Source:"))
                .forEach(n -> assertThat(n.getSlotSharingGroup()).isEqualTo(writerGroup));
        assertThat(writerVertex(graph).getSlotSharingGroup().getResourceProfile())
                .isEqualTo(resources);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testAbsentOptionPreservesExistingResources(boolean explicitHeap) throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        options -> {
                            options.setString("sink.use-managed-memory-allocator", "true");
                            if (explicitHeap) {
                                options.setString("sink.writer-memory", "512 mb");
                            }
                        });
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        write(env, table);
        ResourceProfile resources =
                writerVertex(env.getStreamGraph()).getSlotSharingGroup().getResourceProfile();
        if (explicitHeap) {
            assertThat(resources.getManagedMemory()).isEqualTo(MemorySize.ZERO);
        } else {
            assertThat(resources).isEqualTo(ResourceProfile.UNKNOWN);
        }
    }

    @Test
    void testManagedMemoryRequiresHeap() throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        options -> options.setString("sink.writer-managed-memory", "128 mb"));
        assertThatThrownBy(() -> write(StreamExecutionEnvironment.getExecutionEnvironment(), table))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sink.writer-managed-memory")
                .hasMessageContaining("sink.writer-memory");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testManagedMemoryMustBePositive(boolean managedAllocator) throws Exception {
        FileStoreTable table = createTable("0 b", managedAllocator);
        assertThatThrownBy(() -> write(StreamExecutionEnvironment.getExecutionEnvironment(), table))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sink.writer-managed-memory")
                .hasMessageContaining("positive");
    }

    @Test
    void testExplicitCommitterResourcesArePreserved() throws Exception {
        Options options = new Options();
        options.setString("sink.committer-memory", "64 mb");
        options.setString("sink.committer-cpu", "0.25");
        FileStoreTable table = createTable("128 mb", true).copy(options.toMap());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        write(env, table);
        StreamGraph graph = env.getStreamGraph();
        String group =
                graph.getStreamNodes().stream()
                        .filter(n -> n.getOperatorName().startsWith("Global Committer"))
                        .findFirst()
                        .get()
                        .getSlotSharingGroup();
        ResourceProfile resources = graph.getSlotSharingGroupResource(group).get();
        assertThat(resources.getTaskHeapMemory()).isEqualTo(MemorySize.ofMebiBytes(64));
        assertThat(resources.getManagedMemory()).isEqualTo(MemorySize.ZERO);
        assertThat(resources.getCpuCores().getValue().doubleValue()).isEqualTo(0.25);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testWriterUsesManagedBudgetFromJobGraph(boolean primaryKey) throws Exception {
        FileStoreTable table = createTable("4 mb", true, primaryKey);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Committable> written = write(env, table);
        JobVertex vertex = writerVertex(env.getStreamGraph());
        long managedBytes =
                vertex.getSlotSharingGroup().getResourceProfile().getManagedMemory().getBytes();
        assertThat(managedBytes).isEqualTo(4L * 1024 * 1024);
        StreamConfig config = new StreamConfig(vertex.getConfiguration());
        double fraction =
                config.getManagedMemoryFractionOperatorUseCaseOfSlot(
                        ManagedMemoryUseCase.OPERATOR,
                        new Configuration(),
                        new Configuration(),
                        getClass().getClassLoader());
        assertThat(fraction).isEqualTo(1.0);
        OneInputStreamOperatorFactory<InternalRow, Committable> factory =
                (OneInputStreamOperatorFactory<InternalRow, Committable>)
                        ((OneInputTransformation<InternalRow, Committable>)
                                        written.getTransformation())
                                .getOperatorFactory();
        try (MockEnvironment environment =
                        MockEnvironment.builder().setManagedMemorySize(managedBytes).build();
                OneInputStreamOperatorTestHarness<InternalRow, Committable> harness =
                        new OneInputStreamOperatorTestHarness<>(factory, environment)) {
            // The harness declares STATE_BACKEND by default; the generated writer group does not.
            harness.getStreamConfig().setStateBackendUsesManagedMemory(false);
            harness.getStreamConfig()
                    .setManagedMemoryFractionOperatorOfUseCase(
                            ManagedMemoryUseCase.OPERATOR, fraction);
            harness.setup(new CommittableTypeInfo().createSerializer(new ExecutionConfig()));
            harness.open();
            harness.processElement(GenericRow.of(1, 1L), 1);
            PrepareCommitOperator<?, ?> operator =
                    (PrepareCommitOperator<?, ?>) harness.getOneInputOperator();
            assertThat(operator.memoryPoolFactory.totalBufferSize()).isEqualTo(managedBytes);
            assertThat(operator.memoryPoolFactory.usedBufferSize()).isPositive();
            harness.prepareSnapshotPreBarrier(1);
            assertThat(harness.extractOutputValues()).isNotEmpty();
        }
    }

    private FileStoreTable createTable(String managedMemory, boolean managedAllocator)
            throws Exception {
        return createTable(managedMemory, managedAllocator, false);
    }

    private FileStoreTable createTable(
            String managedMemory, boolean managedAllocator, boolean primaryKey) throws Exception {
        Options options = new Options();
        options.setString("path", tablePath.toString());
        options.setString("bucket", "1");
        options.setString("bucket-key", "a");
        options.setString("sink.writer-cpu", "2.5");
        options.setString("sink.writer-memory", "512 mb");
        options.setString("sink.writer-managed-memory", managedMemory);
        options.setString("sink.use-managed-memory-allocator", String.valueOf(managedAllocator));
        options.setString("write-buffer-for-append", "true");
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.BIGINT())
                        .options(options.toMap());
        if (primaryKey) {
            schema.primaryKey("a");
        }
        new FileSystemSchemaManager(LocalFileIO.create(), tablePath).createTable(schema.build());
        return FileStoreTableFactory.create(LocalFileIO.create(), options);
    }

    private DataStream<Committable> write(StreamExecutionEnvironment env, FileStoreTable table) {
        DataStream<InternalRow> source =
                env.fromData(InternalRow.class, GenericRow.of(1, 1L))
                        .name("upstream")
                        .slotSharingGroup("main");
        FlinkSink<InternalRow> sink = new FixedBucketSink(table, null);
        DataStream<Committable> written = sink.doWrite(source, "test-user", 2);
        sink.doCommit(written, "test-user");
        return written;
    }

    private JobVertex writerVertex(StreamGraph graph) {
        for (JobVertex vertex : graph.getJobGraph().getVertices()) {
            if (vertex.getName().startsWith("Writer")) {
                return vertex;
            }
        }
        throw new AssertionError("Writer vertex not found");
    }
}
