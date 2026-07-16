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

package org.apache.paimon;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.GlobalIndexBuilderUtils;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.ResultEntry;
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
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
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
import org.apache.paimon.vector.index.IvfFlatVectorGlobalIndexerFactory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.DATA_EVOLUTION_ENABLED;
import static org.apache.paimon.CoreOptions.DELETION_VECTORS_ENABLED;
import static org.apache.paimon.CoreOptions.GLOBAL_INDEX_ENABLED;
import static org.apache.paimon.CoreOptions.PATH;
import static org.apache.paimon.CoreOptions.ROW_TRACKING_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Mixed language E2E test for Java paimon-vindex building and Python reading.
 *
 * <p>Java writes data and builds an ivf-flat vector index, then Python reads and searches it.
 */
public class JavaPyE2ETest {

    java.nio.file.Path tempDir = Paths.get("../paimon-python/pypaimon/tests/e2e").toAbsolutePath();

    protected Path warehouse;

    @BeforeEach
    public void before() throws Exception {
        if (!Files.exists(tempDir.resolve("warehouse"))) {
            Files.createDirectories(tempDir.resolve("warehouse"));
        }
        warehouse = new Path(tempDir.resolve("warehouse").toUri());
    }

    @Test
    @EnabledIfSystemProperty(named = "run.e2e.tests", matches = "true")
    public void testVindexVectorIndexWrite() throws Exception {
        String tableName = "test_vindex_vector";
        Path tablePath = new Path(warehouse.toString() + "/default.db/" + tableName);
        LocalFileIO fileIO = LocalFileIO.create();
        if (fileIO.exists(tablePath)) {
            fileIO.delete(tablePath, true);
        }

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
        options.setString(
                IvfFlatVectorGlobalIndexerFactory.IDENTIFIER + ".dimension",
                String.valueOf(dimension));
        options.setString(IvfFlatVectorGlobalIndexerFactory.IDENTIFIER + ".metric", "l2");
        options.setString(IvfFlatVectorGlobalIndexerFactory.IDENTIFIER + ".nlist", "2");

        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(fileIO, tablePath),
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

        DataField embeddingField = table.rowType().getField("embedding");
        Options indexOptions = table.coreOptions().toConfiguration();

        GlobalIndexSingleColumnWriter writer =
                (GlobalIndexSingleColumnWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table,
                                IvfFlatVectorGlobalIndexerFactory.IDENTIFIER,
                                embeddingField,
                                indexOptions);

        for (int i = 0; i < vectors.length; i++) {
            writer.write(vectors[i], i);
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
                        IvfFlatVectorGlobalIndexerFactory.IDENTIFIER,
                        entries);

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

        List<org.apache.paimon.manifest.IndexManifestEntry> indexEntries =
                table.indexManifestFileReader().read(table.latestSnapshot().get().indexManifest());
        assertThat(indexEntries).hasSize(1);
        assertThat(indexEntries.get(0).indexFile().indexType())
                .isEqualTo(IvfFlatVectorGlobalIndexerFactory.IDENTIFIER);
    }

    @Test
    @EnabledIfSystemProperty(named = "run.e2e.tests", matches = "true")
    public void testPrimaryKeyVectorGoldenWrite() throws Exception {
        Path tablePath = new Path(warehouse.toString() + "/default.db/test_pk_vector_golden");
        LocalFileIO fileIO = LocalFileIO.create();
        if (fileIO.exists(tablePath)) {
            fileIO.delete(tablePath, true);
        }

        int dimension = 4;
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT(), DataTypes.VECTOR(dimension, DataTypes.FLOAT())
                        },
                        new String[] {"id", "embedding"});
        Options options = new Options();
        options.set(PATH, tablePath.toString());
        options.set(BUCKET, 1);
        options.set(DELETION_VECTORS_ENABLED, true);
        options.setString(CoreOptions.PK_VECTOR_INDEX_COLUMNS.key(), "embedding");
        options.setString(
                "fields.embedding.pk-vector.index.type",
                IvfFlatVectorGlobalIndexerFactory.IDENTIFIER);
        options.setString(
                IvfFlatVectorGlobalIndexerFactory.IDENTIFIER + ".dimension",
                String.valueOf(dimension));
        options.setString(IvfFlatVectorGlobalIndexerFactory.IDENTIFIER + ".metric", "l2");
        options.setString(IvfFlatVectorGlobalIndexerFactory.IDENTIFIER + ".nlist", "2");

        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(fileIO, tablePath),
                        new Schema(
                                rowType.getFields(),
                                Collections.emptyList(),
                                Collections.singletonList("id"),
                                options.toMap(),
                                ""));
        PrimaryKeyFileStoreTable table =
                new PrimaryKeyFileStoreTable(
                        FileIOFinder.find(tablePath),
                        tablePath,
                        tableSchema,
                        CatalogEnvironment.empty());

        float[][] vectors =
                new float[][] {
                    {1.0f, 0.0f, 0.0f, 0.0f},
                    {0.9f, 0.1f, 0.0f, 0.0f},
                    {0.0f, 1.0f, 0.0f, 0.0f},
                    {0.0f, 0.0f, 1.0f, 0.0f}
                };
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.withIOManager(IOManager.create(warehouse.toString()));
            for (int i = 0; i < vectors.length; i++) {
                write.write(GenericRow.of(i + 1, BinaryVector.fromPrimitiveArray(vectors[i])));
            }
            commit.commit(write.prepareCommit());
        }

        assertThat(
                        table.indexManifestFileReader()
                                .read(table.latestSnapshot().get().indexManifest()))
                .isNotEmpty()
                .allMatch(e -> e.indexFile().globalIndexMeta().sourceMeta() != null);
    }

    @Test
    @EnabledIfSystemProperty(named = "run.e2e.tests", matches = "true")
    public void testVindexVectorRawFallbackWrite() throws Exception {
        String tableName = "test_vindex_vector_raw_fallback";
        Path tablePath = new Path(warehouse.toString() + "/default.db/" + tableName);
        LocalFileIO fileIO = LocalFileIO.create();
        if (fileIO.exists(tablePath)) {
            fileIO.delete(tablePath, true);
        }

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
        options.setString(
                IvfFlatVectorGlobalIndexerFactory.IDENTIFIER + ".dimension",
                String.valueOf(dimension));
        options.setString(IvfFlatVectorGlobalIndexerFactory.IDENTIFIER + ".metric", "l2");
        options.setString(IvfFlatVectorGlobalIndexerFactory.IDENTIFIER + ".nlist", "2");

        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(fileIO, tablePath),
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

        float[][] indexedVectors =
                new float[][] {
                    new float[] {0.0f, 1.0f, 0.0f, 0.0f},
                    new float[] {0.0f, 0.0f, 1.0f, 0.0f},
                    new float[] {0.0f, 0.0f, 0.0f, 1.0f}
                };

        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (int i = 0; i < indexedVectors.length; i++) {
                write.write(GenericRow.of(i, new GenericArray(indexedVectors[i])));
            }
            commit.commit(write.prepareCommit());
        }

        DataField embeddingField = table.rowType().getField("embedding");
        Options indexOptions = table.coreOptions().toConfiguration();

        GlobalIndexSingleColumnWriter writer =
                (GlobalIndexSingleColumnWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table,
                                IvfFlatVectorGlobalIndexerFactory.IDENTIFIER,
                                embeddingField,
                                indexOptions);

        for (int i = 0; i < indexedVectors.length; i++) {
            writer.write(indexedVectors[i], i);
        }

        List<ResultEntry> entries = writer.finish();
        assertThat(entries).hasSize(1);
        assertThat(entries.get(0).rowCount()).isEqualTo(indexedVectors.length);

        Range rowRange = new Range(0, indexedVectors.length - 1);
        List<IndexFileMeta> indexFiles =
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        rowRange,
                        embeddingField.id(),
                        IvfFlatVectorGlobalIndexerFactory.IDENTIFIER,
                        entries);

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

        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(GenericRow.of(3, new GenericArray(new float[] {1.0f, 0.0f, 0.0f, 0.0f})));
            commit.commit(write.prepareCommit());
        }

        List<org.apache.paimon.manifest.IndexManifestEntry> indexEntries =
                table.indexManifestFileReader().read(table.latestSnapshot().get().indexManifest());
        assertThat(indexEntries).hasSize(1);
        assertThat(indexEntries.get(0).indexFile().rowCount()).isEqualTo(indexedVectors.length);
        assertThat(indexEntries.get(0).indexFile().indexType())
                .isEqualTo(IvfFlatVectorGlobalIndexerFactory.IDENTIFIER);
    }
}
