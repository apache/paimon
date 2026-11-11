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
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the writer slot sharing group's task off-heap memory budget. */
class WriterOffHeapMemoryTest extends CommitterTestBase {

    @ParameterizedTest
    @CsvSource({"128 mb, 134217728", "1 gb, 1073741824", "1536 kb, 1572864", "512 kb, 524288"})
    void testOffHeapResourcesPreserveBytes(String offHeapMemory, long expectedBytes)
            throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        options -> {
                            options.setString("sink.writer-memory", "512 mb");
                            options.setString("sink.writer-offheap-memory", offHeapMemory);
                        });
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Committable> written = write(env, table);
        StreamGraph graph = env.getStreamGraph();
        String writerGroup = written.getTransformation().getName();
        ResourceProfile resources = graph.getSlotSharingGroupResource(writerGroup).get();
        assertThat(resources.getTaskOffHeapMemory().getBytes()).isEqualTo(expectedBytes);
        assertThat(resources.getTaskHeapMemory()).isEqualTo(MemorySize.ofMebiBytes(512));
        assertThat(resources.getManagedMemory()).isEqualTo(MemorySize.ZERO);
        assertThat(resources.getCpuCores().getValue().doubleValue()).isEqualTo(1.0);
        graph.getStreamNodes().stream()
                .filter(n -> !n.getOperatorName().startsWith("Source:"))
                .forEach(n -> assertThat(n.getSlotSharingGroup()).isEqualTo(writerGroup));
        assertThat(writerVertex(graph).getSlotSharingGroup().getResourceProfile())
                .isEqualTo(resources);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testOffHeapAndManagedResourcesAreIndependent(boolean managedAllocator) throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        options -> {
                            options.setString("sink.writer-cpu", "2.5");
                            options.setString("sink.writer-memory", "512 mb");
                            options.setString("sink.writer-offheap-memory", "128 mb");
                            options.setString("sink.writer-managed-memory", "64 mb");
                            options.setString(
                                    "sink.use-managed-memory-allocator",
                                    String.valueOf(managedAllocator));
                            options.setString("sink.managed.writer-buffer-memory", "256 mb");
                            options.setString("sink.committer-memory", "32 mb");
                            options.setString("sink.committer-cpu", "0.25");
                        });
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Committable> written = write(env, table);
        StreamGraph graph = env.getStreamGraph();
        ResourceProfile writerResources =
                graph.getSlotSharingGroupResource(written.getTransformation().getName()).get();
        assertThat(writerResources.getTaskOffHeapMemory()).isEqualTo(MemorySize.ofMebiBytes(128));
        assertThat(writerResources.getManagedMemory()).isEqualTo(MemorySize.ofMebiBytes(64));
        assertThat(writerResources.getTaskHeapMemory()).isEqualTo(MemorySize.ofMebiBytes(512));
        assertThat(writerResources.getCpuCores().getValue().doubleValue()).isEqualTo(2.5);
        assertThat(written.getTransformation().getManagedMemoryOperatorScopeUseCaseWeights())
                .isEqualTo(
                        managedAllocator
                                ? Collections.singletonMap(ManagedMemoryUseCase.OPERATOR, 256)
                                : Collections.emptyMap());
        String committerGroup =
                graph.getStreamNodes().stream()
                        .filter(n -> n.getOperatorName().startsWith("Global Committer"))
                        .findFirst()
                        .get()
                        .getSlotSharingGroup();
        ResourceProfile committerResources =
                graph.getSlotSharingGroupResource(committerGroup).get();
        assertThat(committerResources.getTaskOffHeapMemory()).isEqualTo(MemorySize.ZERO);
        assertThat(committerResources.getManagedMemory()).isEqualTo(MemorySize.ZERO);
        assertThat(committerResources.getTaskHeapMemory()).isEqualTo(MemorySize.ofMebiBytes(32));
        assertThat(committerResources.getCpuCores().getValue().doubleValue()).isEqualTo(0.25);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testAbsentOptionPreservesExistingResources(boolean explicitHeap) throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        options -> {
                            if (explicitHeap) {
                                options.setString("sink.writer-memory", "512 mb");
                            }
                        });
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        write(env, table);
        ResourceProfile resources =
                writerVertex(env.getStreamGraph()).getSlotSharingGroup().getResourceProfile();
        if (explicitHeap) {
            assertThat(resources.getTaskOffHeapMemory()).isEqualTo(MemorySize.ZERO);
        } else {
            assertThat(resources).isEqualTo(ResourceProfile.UNKNOWN);
        }
    }

    @Test
    void testLegacyResourceOverloadPreservesManagedMemory() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> operator = env.fromData(1);
        FlinkSink.configureSlotSharingGroup(
                operator,
                2.5,
                org.apache.paimon.options.MemorySize.ofMebiBytes(512),
                org.apache.paimon.options.MemorySize.ofMebiBytes(64),
                null);
        operator.print();
        ResourceProfile resources =
                env.getStreamGraph().getSlotSharingGroupResource(operator.getName()).get();
        assertThat(resources.getTaskOffHeapMemory()).isEqualTo(MemorySize.ZERO);
        assertThat(resources.getManagedMemory()).isEqualTo(MemorySize.ofMebiBytes(64));
        assertThat(resources.getTaskHeapMemory()).isEqualTo(MemorySize.ofMebiBytes(512));
        assertThat(resources.getCpuCores().getValue().doubleValue()).isEqualTo(2.5);
    }

    @Test
    void testOffHeapMemoryRequiresHeap() throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        options -> options.setString("sink.writer-offheap-memory", "128 mb"));
        assertThatThrownBy(() -> write(StreamExecutionEnvironment.getExecutionEnvironment(), table))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sink.writer-offheap-memory")
                .hasMessageContaining("sink.writer-memory");
    }

    @ParameterizedTest
    @ValueSource(strings = {"0 b", "-1 mb", "invalid"})
    void testInvalidOffHeapMemoryIsRejected(String offHeapMemory) throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        options -> {
                            options.setString("sink.writer-memory", "512 mb");
                            options.setString("sink.writer-offheap-memory", offHeapMemory);
                        });
        assertThatThrownBy(() -> write(StreamExecutionEnvironment.getExecutionEnvironment(), table))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sink.writer-offheap-memory");
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
