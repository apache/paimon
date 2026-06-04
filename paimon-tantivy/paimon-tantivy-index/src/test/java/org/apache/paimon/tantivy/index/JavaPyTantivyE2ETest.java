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
import java.util.Arrays;
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
        writeTableWithTantivyIndex(
                "test_tantivy_fulltext",
                Arrays.asList(
                        "Apache Paimon is a streaming data lake platform",
                        "Tantivy is a full-text search engine written in Rust",
                        "Paimon supports real-time data ingestion and analytics",
                        "Full-text search enables efficient text retrieval",
                        "Data lake platforms like Paimon handle large-scale data"),
                "default");

        writeTableWithTantivyIndex(
                "test_tantivy_fulltext_ngram",
                Arrays.asList(
                        "Apache Paimon 支持中文全文检索",
                        "Tantivy ngram tokenizer helps Chinese search",
                        "湖仓表支持实时数据分析",
                        "默认分词适合英文内容",
                        "中文索引支持片段查询"),
                "ngram");

        writeTableWithTantivyIndex(
                "test_tantivy_fulltext_simple",
                Arrays.asList(
                        "Running runners search Apache Paimon",
                        "Run search with Paimon lake",
                        "The connector runs analytics"),
                "simple");

        writeTableWithTantivyIndex(
                "test_tantivy_fulltext_jieba",
                Arrays.asList(
                        "张华在百货公司当售货员",
                        "Apache Paimon supports full text search",
                        "李萍进入中等技术学校学习",
                        "中文分词支持更自然的全文检索",
                        "默认英文分词不适合中文语义"),
                "jieba");
    }

    private void writeTableWithTantivyIndex(
            String tableName, List<String> contents, String tokenizer) throws Exception {
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
            for (int i = 0; i < contents.size(); i++) {
                write.write(GenericRow.of(i, BinaryString.fromString(contents.get(i))));
            }
            commit.commit(write.prepareCommit());
        }

        // Build tantivy full-text index on the "content" column
        DataField contentField = table.rowType().getField("content");
        Options indexOptions = table.coreOptions().toConfiguration();
        if (!"default".equals(tokenizer)) {
            indexOptions.set(TantivyFullTextIndexOptions.TOKENIZER, tokenizer);
        }
        if ("ngram".equals(tokenizer)) {
            indexOptions.set(TantivyFullTextIndexOptions.NGRAM_MIN_GRAM, 2);
            indexOptions.set(TantivyFullTextIndexOptions.NGRAM_MAX_GRAM, 2);
        } else if ("simple".equals(tokenizer)) {
            indexOptions.set(TantivyFullTextIndexOptions.STEM, true);
            indexOptions.set(TantivyFullTextIndexOptions.REMOVE_STOP_WORDS, true);
        }

        GlobalIndexSingletonWriter writer =
                (GlobalIndexSingletonWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table,
                                TantivyFullTextGlobalIndexerFactory.IDENTIFIER,
                                contentField,
                                indexOptions);

        // Write the same text data to the index.
        for (String content : contents) {
            writer.write(BinaryString.fromString(content));
        }

        List<ResultEntry> entries = writer.finish();
        assertThat(entries).hasSize(1);
        assertThat(entries.get(0).rowCount()).isEqualTo(contents.size());
        TantivyFullTextIndexOptions persistedOptions =
                TantivyFullTextIndexOptions.deserialize(entries.get(0).meta());
        assertThat(persistedOptions.tokenizer()).isEqualTo(tokenizer);
        assertThat(persistedOptions.ngramMinGram()).isEqualTo(2);
        assertThat(persistedOptions.ngramMaxGram()).isEqualTo(2);
        if ("simple".equals(tokenizer)) {
            assertThat(persistedOptions.stem()).isTrue();
            assertThat(persistedOptions.removeStopWords()).isTrue();
        }

        Range rowRange = new Range(0, contents.size() - 1);
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
