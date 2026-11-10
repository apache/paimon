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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the committer slot sharing group's memory resources. */
class CommitterMemoryTest extends CommitterTestBase {

    @ParameterizedTest
    @CsvSource({
        "128 mb, , 134217728, 0",
        "1 gb, , 1073741824, 0",
        "1536 kb, , 1572864, 0",
        "512 kb, , 524288, 0",
        ", 128 mb, 0, 134217728",
        ", 1 gb, 0, 1073741824",
        ", 1536 kb, 0, 1572864",
        ", 512 kb, 0, 524288"
    })
    void testMemoryResourcesPreserveBytes(
            String offHeap, String managed, long offHeapBytes, long managedBytes) throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        options -> {
                            options.setString("sink.committer-memory", "256 mb");
                            if (offHeap != null) {
                                options.setString("sink.committer-offheap-memory", offHeap);
                            }
                            if (managed != null) {
                                options.setString("sink.committer-managed-memory", managed);
                            }
                        });
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        write(env, table);
        ResourceProfile resources = committerResources(env.getStreamGraph());
        assertThat(resources.getTaskOffHeapMemory().getBytes()).isEqualTo(offHeapBytes);
        assertThat(resources.getManagedMemory().getBytes()).isEqualTo(managedBytes);
        assertThat(resources.getTaskHeapMemory()).isEqualTo(MemorySize.ofMebiBytes(256));
        assertThat(resources.getCpuCores().getValue().doubleValue()).isEqualTo(1.0);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testWriterAndCommitterResourcesAreIndependent(boolean managedAllocator) throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        options -> {
                            options.setString("sink.writer-cpu", "2.5");
                            options.setString("sink.writer-memory", "512 mb");
                            options.setString("sink.writer-offheap-memory", "32 mb");
                            options.setString("sink.writer-managed-memory", "16 mb");
                            options.setString(
                                    "sink.use-managed-memory-allocator",
                                    String.valueOf(managedAllocator));
                            options.setString("sink.managed.writer-buffer-memory", "256 mb");
                            options.setString("sink.committer-cpu", "0.25");
                            options.setString("sink.committer-memory", "256 mb");
                            options.setString("sink.committer-offheap-memory", "64 mb");
                            options.setString("sink.committer-managed-memory", "128 mb");
                        });
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Committable> written = write(env, table);
        StreamGraph graph = env.getStreamGraph();
        ResourceProfile writer =
                graph.getSlotSharingGroupResource(written.getTransformation().getName()).get();
        assertThat(writer.getCpuCores().getValue().doubleValue()).isEqualTo(2.5);
        assertThat(writer.getTaskHeapMemory()).isEqualTo(MemorySize.ofMebiBytes(512));
        assertThat(writer.getTaskOffHeapMemory()).isEqualTo(MemorySize.ofMebiBytes(32));
        assertThat(writer.getManagedMemory()).isEqualTo(MemorySize.ofMebiBytes(16));
        assertThat(written.getTransformation().getManagedMemoryOperatorScopeUseCaseWeights())
                .isEqualTo(
                        managedAllocator
                                ? Collections.singletonMap(ManagedMemoryUseCase.OPERATOR, 256)
                                : Collections.emptyMap());
        ResourceProfile committer = committerResources(graph);
        assertThat(committer.getCpuCores().getValue().doubleValue()).isEqualTo(0.25);
        assertThat(committer.getTaskHeapMemory()).isEqualTo(MemorySize.ofMebiBytes(256));
        assertThat(committer.getTaskOffHeapMemory()).isEqualTo(MemorySize.ofMebiBytes(64));
        assertThat(committer.getManagedMemory()).isEqualTo(MemorySize.ofMebiBytes(128));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testAbsentOptionsPreserveExistingResources(boolean explicitHeap) throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        options -> {
                            if (explicitHeap) {
                                options.setString("sink.committer-cpu", "0.25");
                                options.setString("sink.committer-memory", "256 mb");
                            }
                        });
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        write(env, table);
        StreamGraph graph = env.getStreamGraph();
        ResourceProfile resources =
                committerVertex(graph).getSlotSharingGroup().getResourceProfile();
        if (explicitHeap) {
            assertThat(resources).isEqualTo(committerResources(graph));
            assertThat(resources.getCpuCores().getValue().doubleValue()).isEqualTo(0.25);
            assertThat(resources.getTaskHeapMemory()).isEqualTo(MemorySize.ofMebiBytes(256));
            assertThat(resources.getTaskOffHeapMemory()).isEqualTo(MemorySize.ZERO);
            assertThat(resources.getManagedMemory()).isEqualTo(MemorySize.ZERO);
        } else {
            assertThat(resources).isEqualTo(ResourceProfile.UNKNOWN);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"sink.committer-offheap-memory", "sink.committer-managed-memory"})
    void testMemoryRequiresCommitterHeap(String option) throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        options -> {
                            // Writer heap must not satisfy the committer's prerequisite.
                            options.setString("sink.writer-memory", "512 mb");
                            options.setString(option, "128 mb");
                        });
        assertThatThrownBy(() -> write(StreamExecutionEnvironment.getExecutionEnvironment(), table))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(option)
                .hasMessageContaining("sink.committer-memory")
                .hasMessageNotContaining("sink.writer-");
    }

    @ParameterizedTest
    @CsvSource({
        "sink.committer-offheap-memory, 0 b",
        "sink.committer-offheap-memory, -1 mb",
        "sink.committer-offheap-memory, invalid",
        "sink.committer-managed-memory, 0 b",
        "sink.committer-managed-memory, -1 mb",
        "sink.committer-managed-memory, invalid"
    })
    void testInvalidMemoryIsRejected(String option, String value) throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        options -> {
                            options.setString("sink.committer-memory", "256 mb");
                            options.setString(option, value);
                        });
        assertThatThrownBy(() -> write(StreamExecutionEnvironment.getExecutionEnvironment(), table))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(option)
                .hasMessageNotContaining("sink.writer-");
    }

    private DataStream<Committable> write(StreamExecutionEnvironment env, FileStoreTable table) {
        DataStream<InternalRow> input =
                env.fromData(InternalRow.class, GenericRow.of(1, 1L)).slotSharingGroup("main");
        FlinkSink<InternalRow> sink = new FixedBucketSink(table, null);
        DataStream<Committable> written = sink.doWrite(input, "test-user", 2);
        assertThat(
                        sink.doCommit(written, "test-user")
                                .getTransformation()
                                .getInputs()
                                .get(0)
                                .getManagedMemoryOperatorScopeUseCaseWeights())
                .isEmpty();
        return written;
    }

    private ResourceProfile committerResources(StreamGraph graph) {
        String group =
                graph.getStreamNodes().stream()
                        .filter(n -> n.getOperatorName().startsWith("Global Committer"))
                        .findFirst()
                        .get()
                        .getSlotSharingGroup();
        ResourceProfile resources = graph.getSlotSharingGroupResource(group).get();
        assertThat(committerVertex(graph).getSlotSharingGroup().getResourceProfile())
                .isEqualTo(resources);
        return resources;
    }

    private JobVertex committerVertex(StreamGraph graph) {
        for (JobVertex vertex : graph.getJobGraph().getVertices()) {
            if (vertex.getName().startsWith("Global Committer")) {
                return vertex;
            }
        }
        throw new AssertionError("Committer vertex not found");
    }
}
