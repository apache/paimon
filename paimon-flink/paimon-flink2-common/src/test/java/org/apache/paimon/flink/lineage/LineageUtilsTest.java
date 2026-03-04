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

package org.apache.paimon.flink.lineage;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.lineage.DatasetConfigFacet;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink.SinkRuntimeProvider;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.ScanTableSource.ScanRuntimeProvider;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LineageUtils}. */
class LineageUtilsTest {

    @TempDir java.nio.file.Path temp;

    private Path tablePath;

    @BeforeEach
    void setUp() {
        tablePath = new Path(temp.toUri().toString());
    }

    private FileStoreTable createTable(
            Map<String, String> options,
            java.util.List<String> partitionKeys,
            java.util.List<String> primaryKeys)
            throws Exception {
        new SchemaManager(LocalFileIO.create(), tablePath)
                .createTable(
                        new Schema(
                                RowType.of(new IntType(), new VarCharType(100), new IntType())
                                        .getFields(),
                                partitionKeys,
                                primaryKeys,
                                options,
                                ""));
        return FileStoreTableFactory.create(LocalFileIO.create(), tablePath);
    }

    @Test
    void testGetNamespace() throws Exception {
        FileStoreTable table =
                createTable(new HashMap<>(), Collections.emptyList(), Arrays.asList("f0"));

        String namespace = LineageUtils.getNamespace(table);

        assertThat(namespace).startsWith("paimon://");
        assertThat(namespace).contains(tablePath.toString());
    }

    @Test
    void testSourceLineageVertexBounded() throws Exception {
        FileStoreTable table =
                createTable(new HashMap<>(), Collections.emptyList(), Arrays.asList("f0"));

        SourceLineageVertex vertex = LineageUtils.sourceLineageVertex("paimon.db.src", true, table);

        assertThat(vertex).isInstanceOf(PaimonSourceLineageVertex.class);
        assertThat(vertex.boundedness()).isEqualTo(Boundedness.BOUNDED);
        assertThat(vertex.datasets()).hasSize(1);

        LineageDataset dataset = vertex.datasets().get(0);
        assertThat(dataset.name()).isEqualTo("paimon.db.src");
        assertThat(dataset.namespace()).startsWith("paimon://");
    }

    @Test
    void testSourceLineageVertexUnbounded() throws Exception {
        FileStoreTable table =
                createTable(new HashMap<>(), Collections.emptyList(), Arrays.asList("f0"));

        SourceLineageVertex vertex =
                LineageUtils.sourceLineageVertex("paimon.db.src", false, table);

        assertThat(vertex.boundedness()).isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);
    }

    @Test
    void testSinkLineageVertex() throws Exception {
        FileStoreTable table =
                createTable(new HashMap<>(), Collections.emptyList(), Arrays.asList("f0"));

        LineageVertex vertex = LineageUtils.sinkLineageVertex("paimon.db.sink", table);

        assertThat(vertex).isInstanceOf(PaimonSinkLineageVertex.class);
        assertThat(vertex.datasets()).hasSize(1);

        LineageDataset dataset = vertex.datasets().get(0);
        assertThat(dataset.name()).isEqualTo("paimon.db.sink");
        assertThat(dataset.namespace()).startsWith("paimon://");
    }

    @Test
    void testConfigFacetContainsPartitionAndPrimaryKeys() throws Exception {
        FileStoreTable table =
                createTable(new HashMap<>(), Arrays.asList("f2"), Arrays.asList("f0", "f2"));

        LineageVertex vertex = LineageUtils.sinkLineageVertex("paimon.db.t", table);
        LineageDataset dataset = vertex.datasets().get(0);

        Map<String, LineageDatasetFacet> facets = dataset.facets();
        assertThat(facets).containsKey("config");

        DatasetConfigFacet configFacet = (DatasetConfigFacet) facets.get("config");
        Map<String, String> config = configFacet.config();
        assertThat(config).containsEntry("partition-keys", "f2");
        assertThat(config).containsEntry("primary-keys", "f0,f2");
    }

    @Test
    void testConfigFacetIncludesPaimonOptions() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.CHANGELOG_PRODUCER.key(), "lookup");

        FileStoreTable table = createTable(options, Collections.emptyList(), Arrays.asList("f0"));

        SourceLineageVertex vertex = LineageUtils.sourceLineageVertex("paimon.db.t", true, table);
        LineageDataset dataset = vertex.datasets().get(0);

        DatasetConfigFacet configFacet = (DatasetConfigFacet) dataset.facets().get("config");
        Map<String, String> config = configFacet.config();
        assertThat(config).containsEntry(CoreOptions.CHANGELOG_PRODUCER.key(), "lookup");
    }

    @Test
    void testConfigFacetWithEmptyKeys() throws Exception {
        FileStoreTable table =
                createTable(new HashMap<>(), Collections.emptyList(), Collections.emptyList());

        LineageVertex vertex = LineageUtils.sinkLineageVertex("paimon.db.t", table);
        LineageDataset dataset = vertex.datasets().get(0);

        DatasetConfigFacet configFacet = (DatasetConfigFacet) dataset.facets().get("config");
        Map<String, String> config = configFacet.config();
        assertThat(config).containsEntry("partition-keys", "");
        assertThat(config).containsEntry("primary-keys", "");
    }

    @Test
    void testGetScanProviderImplementsLineageVertexProvider() throws Exception {
        FileStoreTable table =
                createTable(new HashMap<>(), Collections.emptyList(), Arrays.asList("f0"));

        DataStreamScanProvider stub =
                new DataStreamScanProvider() {
                    @Override
                    public DataStream<RowData> produceDataStream(
                            org.apache.flink.table.connector.ProviderContext context,
                            StreamExecutionEnvironment env) {
                        return null;
                    }

                    @Override
                    public boolean isBounded() {
                        return true;
                    }
                };
        ScanRuntimeProvider wrapped =
                DataStreamProviderFactory.getScanProvider(stub, "paimon.db.src", table);

        assertThat(wrapped).isInstanceOf(LineageVertexProvider.class);
        LineageVertex vertex = ((LineageVertexProvider) wrapped).getLineageVertex();
        assertThat(vertex).isInstanceOf(SourceLineageVertex.class);
        assertThat(vertex.datasets()).hasSize(1);
        assertThat(vertex.datasets().get(0).name()).isEqualTo("paimon.db.src");
    }

    @Test
    void testGetSinkProviderImplementsLineageVertexProvider() throws Exception {
        FileStoreTable table =
                createTable(new HashMap<>(), Collections.emptyList(), Arrays.asList("f0"));

        DataStreamSinkProvider stub = (providerContext, dataStream) -> null;
        SinkRuntimeProvider wrapped =
                DataStreamProviderFactory.getSinkProvider(stub, "paimon.db.sink", table);

        assertThat(wrapped).isInstanceOf(LineageVertexProvider.class);
        LineageVertex vertex = ((LineageVertexProvider) wrapped).getLineageVertex();
        assertThat(vertex.datasets()).hasSize(1);
        assertThat(vertex.datasets().get(0).name()).isEqualTo("paimon.db.sink");
    }
}
