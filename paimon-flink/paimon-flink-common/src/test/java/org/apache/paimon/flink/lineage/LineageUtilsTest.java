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
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.flink.PaimonDataStreamScanProvider;
import org.apache.paimon.flink.sink.PaimonDiscardingSink;
import org.apache.paimon.flink.source.ContinuousFileStoreSource;
import org.apache.paimon.flink.source.PaimonDataStreamSource;
import org.apache.paimon.flink.source.operator.MonitorSource;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.lineage.DatasetConfigFacet;
import org.apache.flink.streaming.api.lineage.DatasetSchemaFacet;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;
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

    private FileStoreTable createTableWithCatalogOptions(Map<String, String> catalogOptions)
            throws Exception {
        FileStoreTable table =
                createTable(new HashMap<>(), Collections.emptyList(), Arrays.asList("f0"));
        CatalogEnvironment catalogEnvironment =
                new CatalogEnvironment(
                        null,
                        null,
                        null,
                        null,
                        null,
                        CatalogContext.create(Options.fromMap(catalogOptions)),
                        false,
                        false);
        return FileStoreTableFactory.create(
                LocalFileIO.create(), tablePath, table.schema(), catalogEnvironment);
    }

    @Test
    void testGetNamespaceWithWarehouse() throws Exception {
        Map<String, String> catalogOptions = new HashMap<>();
        catalogOptions.put("warehouse", "s3://my-bucket/warehouse");
        FileStoreTable table = createTableWithCatalogOptions(catalogOptions);

        String namespace =
                LineageUtils.getNamespace(table, table.catalogEnvironment().catalogContext());

        assertThat(namespace).isEqualTo("s3://my-bucket/warehouse");
    }

    @Test
    void testGetNamespaceFallsBackToPath() throws Exception {
        FileStoreTable table =
                createTable(new HashMap<>(), Collections.emptyList(), Arrays.asList("f0"));

        String namespace = LineageUtils.getNamespace(table, null);

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
        assertThat(dataset.namespace()).contains(tablePath.toString());
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
    void testSourceLineageVertexKeepsProvidedNameWhenCatalogKeyExists() throws Exception {
        Map<String, String> catalogOptions = new HashMap<>();
        catalogOptions.put("catalog-key", "jdbc-warehouse");
        FileStoreTable table = createTableWithCatalogOptions(catalogOptions);

        SourceLineageVertex vertex = LineageUtils.sourceLineageVertex("paimon.db.src", true, table);

        assertThat(vertex.datasets().get(0).name()).isEqualTo("paimon.db.src");
    }

    @Test
    void testResolveNameByMetastoreUsesCatalogKeyForJdbcMetastore() throws Exception {
        Map<String, String> catalogOptions = new HashMap<>();
        catalogOptions.put("metastore", "jdbc");
        catalogOptions.put("catalog-key", "jdbc-warehouse");
        FileStoreTable table = createTableWithCatalogOptions(catalogOptions);

        // catalog-key is only used when no explicit name is provided (DataStream path)
        assertThat(LineageUtils.resolveNameByMetastore(table, null))
                .isEqualTo("jdbc-warehouse." + table.fullName());
        // when explicit name is provided (Table API path), it takes precedence
        assertThat(LineageUtils.resolveNameByMetastore(table, "my_catalog.db.src"))
                .isEqualTo("my_catalog.db.src");
    }

    @Test
    void testResolveNameByMetastoreIgnoresCatalogKeyForNonJdbcMetastore() throws Exception {
        Map<String, String> catalogOptions = new HashMap<>();
        catalogOptions.put("catalog-key", "jdbc-warehouse");
        FileStoreTable table = createTableWithCatalogOptions(catalogOptions);

        assertThat(LineageUtils.resolveNameByMetastore(table, "paimon.db.src"))
                .isEqualTo("paimon.db.src");
    }

    @Test
    void testDataStreamSourceLineageVertexUsesCatalogKey() throws Exception {
        Map<String, String> catalogOptions = new HashMap<>();
        catalogOptions.put("metastore", "jdbc");
        catalogOptions.put("catalog-key", "jdbc-warehouse");
        FileStoreTable table = createTableWithCatalogOptions(catalogOptions);

        SourceLineageVertex vertex = LineageUtils.sourceLineageVertex(true, table);

        assertThat(vertex.datasets().get(0).name()).isEqualTo("jdbc-warehouse." + table.fullName());
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
        assertThat(dataset.namespace()).contains(tablePath.toString());
    }

    @Test
    void testDataStreamSinkLineageVertexUsesCatalogKey() throws Exception {
        Map<String, String> catalogOptions = new HashMap<>();
        catalogOptions.put("metastore", "jdbc");
        catalogOptions.put("catalog-key", "jdbc-warehouse");
        FileStoreTable table = createTableWithCatalogOptions(catalogOptions);

        LineageVertex vertex = LineageUtils.sinkLineageVertex(table);

        assertThat(vertex.datasets().get(0).name()).isEqualTo("jdbc-warehouse." + table.fullName());
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
    void testConfigFacetIncludesIcebergOptionsAndExcludesArbitrary() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("metadata.iceberg.storage", "hadoop-catalog");
        options.put("metadata.iceberg.uri", "s3://my-bucket/iceberg");
        options.put("metadata.iceberg.rest.token", "should-not-appear");
        options.put("some.arbitrary.secret", "should-not-appear");

        FileStoreTable table = createTable(options, Collections.emptyList(), Arrays.asList("f0"));

        LineageVertex vertex = LineageUtils.sinkLineageVertex("paimon.db.t", table);
        LineageDataset dataset = vertex.datasets().get(0);

        DatasetConfigFacet configFacet = (DatasetConfigFacet) dataset.facets().get("config");
        Map<String, String> config = configFacet.config();
        assertThat(config).containsEntry("metadata.iceberg.storage", "hadoop-catalog");
        assertThat(config).containsEntry("metadata.iceberg.uri", "s3://my-bucket/iceberg");
        assertThat(config).doesNotContainKey("metadata.iceberg.rest.token");
        assertThat(config).doesNotContainKey("some.arbitrary.secret");
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
    void testSchemaFacetContainsPaimonFields() throws Exception {
        FileStoreTable table =
                createTable(new HashMap<>(), Collections.emptyList(), Arrays.asList("f0"));

        LineageVertex vertex = LineageUtils.sinkLineageVertex("paimon.db.t", table);
        LineageDataset dataset = vertex.datasets().get(0);

        Map<String, LineageDatasetFacet> facets = dataset.facets();
        assertThat(facets).containsKey("schema");

        DatasetSchemaFacet schemaFacet = (DatasetSchemaFacet) facets.get("schema");
        assertThat(schemaFacet.fields()).containsOnlyKeys("f0", "f1", "f2");
        assertThat(schemaFacet.fields().get("f0").type()).isEqualTo("INT NOT NULL");
        assertThat(schemaFacet.fields().get("f1").type()).isEqualTo("VARCHAR(100)");
        assertThat(schemaFacet.fields().get("f2").type()).isEqualTo("INT");
    }

    @Test
    void testScanProviderImplementsLineageVertexProvider() throws Exception {
        FileStoreTable table =
                createTable(new HashMap<>(), Collections.emptyList(), Arrays.asList("f0"));

        PaimonDataStreamScanProvider provider =
                new PaimonDataStreamScanProvider(true, env -> null, "paimon.db.src", table);

        assertThat(provider).isInstanceOf(LineageVertexProvider.class);
        LineageVertex vertex = provider.getLineageVertex();
        assertThat(vertex).isInstanceOf(SourceLineageVertex.class);
        assertThat(vertex.datasets()).hasSize(1);
        assertThat(vertex.datasets().get(0).name()).isEqualTo("paimon.db.src");
    }

    @Test
    void testSinkLineageViaPaimonDiscardingSink() throws Exception {
        FileStoreTable table =
                createTable(new HashMap<>(), Collections.emptyList(), Arrays.asList("f0"));

        PaimonDiscardingSink<?> sink = new PaimonDiscardingSink<>(table);

        assertThat(sink).isInstanceOf(LineageVertexProvider.class);
        LineageVertex vertex = sink.getLineageVertex();
        assertThat(vertex.datasets()).hasSize(1);
    }

    @Test
    void testPaimonDataStreamSourceWrapsMonitorSourceLineageVertex() throws Exception {
        FileStoreTable table =
                createTable(new HashMap<>(), Collections.emptyList(), Arrays.asList("f0"));

        PaimonDataStreamSource<?, ?, ?> source =
                new PaimonDataStreamSource<>(
                        new MonitorSource(table.newReadBuilder(), 10, false, true), table);

        assertThat(source).isInstanceOf(LineageVertexProvider.class);
        SourceLineageVertex vertex = (SourceLineageVertex) source.getLineageVertex();
        assertThat(vertex.boundedness()).isEqualTo(Boundedness.BOUNDED);
        assertThat(vertex.datasets()).hasSize(1);
        assertThat(vertex.datasets().get(0).name()).isEqualTo("paimon." + table.fullName());
    }

    @Test
    void testPaimonDataStreamSourceWrapsFlinkSourceLineageVertex() throws Exception {
        FileStoreTable table =
                createTable(new HashMap<>(), Collections.emptyList(), Arrays.asList("f0"));

        PaimonDataStreamSource<?, ?, ?> source =
                new PaimonDataStreamSource<>(
                        new ContinuousFileStoreSource(
                                table.newReadBuilder(), table.options(), null),
                        table);

        SourceLineageVertex vertex = (SourceLineageVertex) source.getLineageVertex();
        assertThat(vertex.boundedness()).isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);
        assertThat(vertex.datasets()).hasSize(1);
        assertThat(vertex.datasets().get(0).name()).isEqualTo("paimon." + table.fullName());
    }
}
