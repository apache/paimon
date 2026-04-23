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

package org.apache.paimon.lumina.index;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.GlobalIndexBuilderUtils;
import org.apache.paimon.globalindex.GlobalIndexParallelWriter;
import org.apache.paimon.globalindex.GlobalIndexSingletonWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.btree.BTreeGlobalIndexerFactory;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

import org.aliyun.lumina.Lumina;
import org.aliyun.lumina.LuminaException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.CoreOptions.DATA_EVOLUTION_ENABLED;
import static org.apache.paimon.CoreOptions.GLOBAL_INDEX_ENABLED;
import static org.apache.paimon.CoreOptions.PATH;
import static org.apache.paimon.CoreOptions.ROW_TRACKING_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Mixed language E2E test for Java Lumina vector index building and Python reading.
 *
 * <p>Java writes data and builds a Lumina vector index, then Python reads and searches it.
 */
public class JavaPyLuminaE2ETest {

    @BeforeAll
    public static void checkNativeLibrary() {
        if (!Lumina.isLibraryLoaded()) {
            try {
                Lumina.loadLibrary();
            } catch (LuminaException e) {
                assumeTrue(false, "Lumina native library not available: " + e.getMessage());
            }
        }
    }

    java.nio.file.Path tempDir = Paths.get("../paimon-python/pypaimon/tests/e2e").toAbsolutePath();

    protected Path warehouse;

    @BeforeEach
    public void before() throws Exception {
        if (!Files.exists(tempDir.resolve("warehouse"))) {
            Files.createDirectories(tempDir.resolve("warehouse"));
        }
        warehouse = new Path("file://" + tempDir.resolve("warehouse"));
    }

    @Test
    @EnabledIfSystemProperty(named = "run.e2e.tests", matches = "true")
    public void testLuminaVectorIndexWrite() throws Exception {
        String tableName = "test_lumina_vector";
        Path tablePath = new Path(warehouse.toString() + "/default.db/" + tableName);

        int dimension = 4;

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), new ArrayType(new FloatType())},
                        new String[] {"id", "embedding"});

        Options options = new Options();
        options.set(PATH, tablePath.toString());
        options.set(ROW_TRACKING_ENABLED, true);
        options.set(DATA_EVOLUTION_ENABLED, true);
        options.set(GLOBAL_INDEX_ENABLED, true);
        options.setString(LuminaVectorIndexOptions.DIMENSION.key(), String.valueOf(dimension));
        options.setString(LuminaVectorIndexOptions.DISTANCE_METRIC.key(), "l2");
        options.setString(LuminaVectorIndexOptions.ENCODING_TYPE.key(), "rawf32");

        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(LocalFileIO.create(), tablePath),
                        new Schema(
                                rowType.getFields(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                options.toMap(),
                                ""));

        AppendOnlyFileStoreTable table =
                new AppendOnlyFileStoreTable(
                        FileIOFinder.find(tablePath),
                        tablePath,
                        tableSchema,
                        CatalogEnvironment.empty());

        // Test vectors: 6 vectors of dimension 4
        float[][] vectors =
                new float[][] {
                    new float[] {1.0f, 0.0f, 0.0f, 0.0f},
                    new float[] {0.9f, 0.1f, 0.0f, 0.0f},
                    new float[] {0.0f, 1.0f, 0.0f, 0.0f},
                    new float[] {0.0f, 0.0f, 1.0f, 0.0f},
                    new float[] {0.0f, 0.0f, 0.0f, 1.0f},
                    new float[] {0.95f, 0.05f, 0.0f, 0.0f}
                };

        // Write data rows
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (int i = 0; i < vectors.length; i++) {
                write.write(GenericRow.of(i, new GenericArray(vectors[i])));
            }
            commit.commit(write.prepareCommit());
        }

        // Build Lumina vector index on "embedding" column
        DataField embeddingField = table.rowType().getField("embedding");
        Options indexOptions = table.coreOptions().toConfiguration();
        LuminaVectorIndexOptions luminaOptions = new LuminaVectorIndexOptions(indexOptions);

        GlobalIndexSingletonWriter writer =
                (GlobalIndexSingletonWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table,
                                LuminaVectorGlobalIndexerFactory.IDENTIFIER,
                                embeddingField,
                                indexOptions);

        // Write vectors to index
        for (float[] vec : vectors) {
            writer.write(vec);
        }

        List<ResultEntry> entries = writer.finish();
        assertThat(entries).hasSize(1);
        assertThat(entries.get(0).rowCount()).isEqualTo(vectors.length);

        Range rowRange = new Range(0, vectors.length - 1);
        List<IndexFileMeta> indexFiles =
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        rowRange,
                        embeddingField.id(),
                        LuminaVectorGlobalIndexerFactory.IDENTIFIER,
                        entries);

        // Commit the index
        DataIncrement dataIncrement = DataIncrement.indexIncrement(indexFiles);
        CommitMessage message =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        null,
                        dataIncrement,
                        CompactIncrement.emptyIncrement());
        try (BatchTableCommit commit = writeBuilder.newCommit()) {
            commit.commit(Collections.singletonList(message));
        }

        // Verify index was committed
        List<org.apache.paimon.manifest.IndexManifestEntry> indexEntries =
                table.indexManifestFileReader().read(table.latestSnapshot().get().indexManifest());
        assertThat(indexEntries).hasSize(1);
        assertThat(indexEntries.get(0).indexFile().indexType())
                .isEqualTo(LuminaVectorGlobalIndexerFactory.IDENTIFIER);
    }

    @Test
    @EnabledIfSystemProperty(named = "run.e2e.tests", matches = "true")
    public void testLuminaVectorWithBTreeIndexWrite() throws Exception {
        String tableName = "test_lumina_vector_btree_filter";
        Path tablePath = new Path(warehouse.toString() + "/default.db/" + tableName);

        int dimension = 4;

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), new ArrayType(new FloatType())},
                        new String[] {"id", "embedding"});

        Options options = new Options();
        options.set(PATH, tablePath.toString());
        options.set(ROW_TRACKING_ENABLED, true);
        options.set(DATA_EVOLUTION_ENABLED, true);
        options.set(GLOBAL_INDEX_ENABLED, true);
        options.setString(LuminaVectorIndexOptions.DIMENSION.key(), String.valueOf(dimension));
        options.setString(LuminaVectorIndexOptions.DISTANCE_METRIC.key(), "l2");
        options.setString(LuminaVectorIndexOptions.ENCODING_TYPE.key(), "rawf32");

        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(LocalFileIO.create(), tablePath),
                        new Schema(
                                rowType.getFields(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                options.toMap(),
                                ""));

        AppendOnlyFileStoreTable table =
                new AppendOnlyFileStoreTable(
                        FileIOFinder.find(tablePath),
                        tablePath,
                        tableSchema,
                        CatalogEnvironment.empty());

        // Same 6 vectors as testLuminaVectorIndexWrite, paired with ids 0..5.
        float[][] vectors =
                new float[][] {
                    new float[] {1.0f, 0.0f, 0.0f, 0.0f},
                    new float[] {0.9f, 0.1f, 0.0f, 0.0f},
                    new float[] {0.0f, 1.0f, 0.0f, 0.0f},
                    new float[] {0.0f, 0.0f, 1.0f, 0.0f},
                    new float[] {0.0f, 0.0f, 0.0f, 1.0f},
                    new float[] {0.95f, 0.05f, 0.0f, 0.0f}
                };

        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (int i = 0; i < vectors.length; i++) {
                write.write(GenericRow.of(i, new GenericArray(vectors[i])));
            }
            commit.commit(write.prepareCommit());
        }

        Options indexOptions = table.coreOptions().toConfiguration();
        Range rowRange = new Range(0, vectors.length - 1);

        // Build Lumina vector index on "embedding".
        DataField embeddingField = table.rowType().getField("embedding");
        GlobalIndexSingletonWriter vectorWriter =
                (GlobalIndexSingletonWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table,
                                LuminaVectorGlobalIndexerFactory.IDENTIFIER,
                                embeddingField,
                                indexOptions);
        for (float[] vec : vectors) {
            vectorWriter.write(vec);
        }
        List<ResultEntry> vectorEntries = vectorWriter.finish();
        List<IndexFileMeta> vectorIndexFiles =
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        rowRange,
                        embeddingField.id(),
                        LuminaVectorGlobalIndexerFactory.IDENTIFIER,
                        vectorEntries);

        // Build BTree global index on "id".
        DataField idField = table.rowType().getField("id");
        GlobalIndexParallelWriter idWriter =
                (GlobalIndexParallelWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table, BTreeGlobalIndexerFactory.IDENTIFIER, idField, indexOptions);
        for (int i = 0; i < vectors.length; i++) {
            idWriter.write(i, i);
        }
        List<ResultEntry> idEntries = idWriter.finish();
        List<IndexFileMeta> idIndexFiles =
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        rowRange,
                        idField.id(),
                        BTreeGlobalIndexerFactory.IDENTIFIER,
                        idEntries);

        // Commit both index sets in a single index-only commit.
        java.util.List<IndexFileMeta> allIndexFiles = new java.util.ArrayList<>();
        allIndexFiles.addAll(vectorIndexFiles);
        allIndexFiles.addAll(idIndexFiles);
        DataIncrement dataIncrement = DataIncrement.indexIncrement(allIndexFiles);
        CommitMessage message =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        null,
                        dataIncrement,
                        CompactIncrement.emptyIncrement());
        try (BatchTableCommit commit = writeBuilder.newCommit()) {
            commit.commit(Collections.singletonList(message));
        }

        // Verify both index types are present.
        List<org.apache.paimon.manifest.IndexManifestEntry> indexEntries =
                table.indexManifestFileReader().read(table.latestSnapshot().get().indexManifest());
        assertThat(indexEntries).hasSize(2);
        assertThat(
                        indexEntries.stream()
                                .map(e -> e.indexFile().indexType())
                                .collect(java.util.stream.Collectors.toSet()))
                .containsExactlyInAnyOrder(
                        LuminaVectorGlobalIndexerFactory.IDENTIFIER,
                        BTreeGlobalIndexerFactory.IDENTIFIER);
    }
}
