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

package org.apache.paimon.tantivy.index;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.GlobalIndexBuilderUtils;
import org.apache.paimon.globalindex.GlobalIndexSingletonWriter;
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
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.tantivy.NativeLoader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

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
 * Mixed language E2E test for Java Tantivy full-text index building and Python reading.
 *
 * <p>Java writes data and builds a Tantivy full-text index, then Python reads and searches it.
 */
public class JavaPyTantivyE2ETest {

    @BeforeAll
    public static void checkNativeLibrary() {
        assumeTrue(isNativeAvailable(), "Tantivy native library not available, skipping tests");
    }

    private static boolean isNativeAvailable() {
        try {
            NativeLoader.loadJni();
            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    java.nio.file.Path tempDir =
            Paths.get("../../paimon-python/pypaimon/tests/e2e").toAbsolutePath();

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
    public void testTantivyFullTextIndexWrite() throws Exception {
        String tableName = "test_tantivy_fulltext";
        Path tablePath = new Path(warehouse.toString() + "/default.db/" + tableName);

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING()},
                        new String[] {"id", "content"});

        Options options = new Options();
        options.set(PATH, tablePath.toString());
        options.set(ROW_TRACKING_ENABLED, true);
        options.set(DATA_EVOLUTION_ENABLED, true);
        options.set(GLOBAL_INDEX_ENABLED, true);

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

        // Write data
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(
                    GenericRow.of(
                            0,
                            BinaryString.fromString(
                                    "Apache Paimon is a streaming data lake platform")));
            write.write(
                    GenericRow.of(
                            1,
                            BinaryString.fromString(
                                    "Tantivy is a full-text search engine written in Rust")));
            write.write(
                    GenericRow.of(
                            2,
                            BinaryString.fromString(
                                    "Paimon supports real-time data ingestion and analytics")));
            write.write(
                    GenericRow.of(
                            3,
                            BinaryString.fromString(
                                    "Full-text search enables efficient text retrieval")));
            write.write(
                    GenericRow.of(
                            4,
                            BinaryString.fromString(
                                    "Data lake platforms like Paimon handle large-scale data")));
            commit.commit(write.prepareCommit());
        }

        // Build tantivy full-text index on the "content" column
        DataField contentField = table.rowType().getField("content");
        Options indexOptions = table.coreOptions().toConfiguration();

        GlobalIndexSingletonWriter writer =
                (GlobalIndexSingletonWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table,
                                TantivyFullTextGlobalIndexerFactory.IDENTIFIER,
                                contentField,
                                indexOptions);

        // Write the same text data to the index
        writer.write(BinaryString.fromString("Apache Paimon is a streaming data lake platform"));
        writer.write(
                BinaryString.fromString("Tantivy is a full-text search engine written in Rust"));
        writer.write(
                BinaryString.fromString("Paimon supports real-time data ingestion and analytics"));
        writer.write(BinaryString.fromString("Full-text search enables efficient text retrieval"));
        writer.write(
                BinaryString.fromString("Data lake platforms like Paimon handle large-scale data"));

        List<ResultEntry> entries = writer.finish();
        assertThat(entries).hasSize(1);
        assertThat(entries.get(0).rowCount()).isEqualTo(5);

        Range rowRange = new Range(0, 4);
        List<IndexFileMeta> indexFiles =
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        rowRange,
                        contentField.id(),
                        TantivyFullTextGlobalIndexerFactory.IDENTIFIER,
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

        // Verify the index was committed
        List<org.apache.paimon.manifest.IndexManifestEntry> indexEntries =
                table.indexManifestFileReader().read(table.latestSnapshot().get().indexManifest());
        assertThat(indexEntries).hasSize(1);
        assertThat(indexEntries.get(0).indexFile().indexType())
                .isEqualTo(TantivyFullTextGlobalIndexerFactory.IDENTIFIER);
    }
}
