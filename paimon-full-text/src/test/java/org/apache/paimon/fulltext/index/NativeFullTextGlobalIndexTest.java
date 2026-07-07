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

package org.apache.paimon.fulltext.index;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FullTextSearch;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.apache.paimon.shade.guava30.com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Test for {@link NativeFullTextGlobalIndexWriter} and {@link NativeFullTextGlobalIndexReader}. */
public class NativeFullTextGlobalIndexTest {

    private static boolean isNativeAvailable() {
        String path = System.getenv("PAIMON_FTINDEX_JNI_LIB_PATH");
        return path != null && !path.isEmpty() && new File(path).isFile();
    }

    @TempDir java.nio.file.Path tempDir;

    private FileIO fileIO;
    private Path indexPath;

    @BeforeEach
    public void setup() {
        assumeTrue(isNativeAvailable(), "Native full-text library not available, skipping tests");
        fileIO = new LocalFileIO();
        indexPath = new Path(tempDir.toString());
    }

    @AfterEach
    public void cleanup() throws IOException {
        if (fileIO != null) {
            fileIO.delete(indexPath, true);
        }
    }

    private GlobalIndexFileWriter createFileWriter(Path path) {
        return new GlobalIndexFileWriter() {
            @Override
            public String newFileName(String prefix) {
                return prefix + "-" + UUID.randomUUID();
            }

            @Override
            public PositionOutputStream newOutputStream(String fileName) throws IOException {
                return fileIO.newOutputStream(new Path(path, fileName), false);
            }
        };
    }

    private GlobalIndexFileReader createFileReader() {
        return meta -> fileIO.newInputStream(meta.filePath());
    }

    private List<GlobalIndexIOMeta> toIOMetas(List<ResultEntry> results, Path path)
            throws IOException {
        assertThat(results).hasSize(1);
        ResultEntry result = results.get(0);
        Path filePath = new Path(path, result.fileName());
        return Collections.singletonList(
                new GlobalIndexIOMeta(filePath, fileIO.getFileSize(filePath), result.meta()));
    }

    private NativeFullTextGlobalIndexReader createReader(
            GlobalIndexFileReader fileReader, List<GlobalIndexIOMeta> metas) {
        return new NativeFullTextGlobalIndexReader(fileReader, metas, newDirectExecutorService());
    }

    private NativeFullTextIndexOptions indexOptions(Options options) {
        return new NativeFullTextIndexOptions(
                options.removePrefix(NativeFullTextIndexOptions.FULL_TEXT_PREFIX).toMap());
    }

    private Map<String, String> serializedOptions(byte[] metadata) {
        return JsonSerdeUtil.fromJson(
                new String(metadata, StandardCharsets.UTF_8),
                new TypeReference<Map<String, String>>() {});
    }

    @Test
    public void testEndToEnd() throws IOException {
        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        NativeFullTextGlobalIndexWriter writer = new NativeFullTextGlobalIndexWriter(fileWriter);

        writer.write(BinaryString.fromString("Apache Paimon is a streaming data lake platform"), 0);
        writer.write(BinaryString.fromString("Native full-text search runs in Rust"), 1);
        writer.write(BinaryString.fromString("Paimon supports real-time data ingestion"), 2);

        List<ResultEntry> results = writer.finish();
        assertThat(results).hasSize(1);
        assertThat(results.get(0).rowCount()).isEqualTo(3);

        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);
        GlobalIndexFileReader fileReader = createFileReader();

        try (NativeFullTextGlobalIndexReader reader = createReader(fileReader, metas)) {
            FullTextSearch search = new FullTextSearch("text", matchQuery("paimon"), 10);
            Optional<ScoredGlobalIndexResult> searchResult =
                    reader.visitFullTextSearch(search).join();
            assertThat(searchResult).isPresent();

            ScoredGlobalIndexResult scored = searchResult.get();
            RoaringNavigableMap64 rowIds = scored.results();
            // Row 0 and row 2 mention "paimon"
            assertThat(rowIds.getLongCardinality()).isEqualTo(2);
            assertThat(rowIds.contains(0L)).isTrue();
            assertThat(rowIds.contains(2L)).isTrue();

            // Scores should be positive
            float score0 = scored.scoreGetter().score(0L);
            float score2 = scored.scoreGetter().score(2L);
            assertThat(score0).isGreaterThan(0);
            assertThat(score2).isGreaterThan(0);
        }
    }

    @Test
    public void testWriterPersistsTokenizerMeta() throws IOException {
        Options options = new Options();
        options.set("full-text.tokenizer", "ngram");
        options.set("full-text.ngram.min-gram", "2");
        options.set("full-text.ngram.max-gram", "2");

        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        NativeFullTextGlobalIndexWriter writer =
                new NativeFullTextGlobalIndexWriter(fileWriter, indexOptions(options));

        writer.write(BinaryString.fromString("Apache Paimon supports Chinese text"), 0);
        List<ResultEntry> results = writer.finish();

        assertThat(results).hasSize(1);
        Map<String, String> indexOptions = serializedOptions(results.get(0).meta());
        assertThat(indexOptions)
                .containsEntry("tokenizer", "ngram")
                .containsEntry("ngram.min-gram", "2")
                .containsEntry("ngram.max-gram", "2");
    }

    @Test
    public void testJiebaTokenizerFindsChineseWord() throws IOException {
        Options options = new Options();
        options.set("full-text.tokenizer", "jieba");

        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        NativeFullTextGlobalIndexWriter writer =
                new NativeFullTextGlobalIndexWriter(fileWriter, indexOptions(options));

        writer.write(BinaryString.fromString("张华在百货公司当售货员"), 0);
        writer.write(BinaryString.fromString("Apache Paimon supports full text search"), 1);

        List<ResultEntry> results = writer.finish();
        assertThat(serializedOptions(results.get(0).meta())).containsEntry("tokenizer", "jieba");

        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);
        GlobalIndexFileReader fileReader = createFileReader();

        try (NativeFullTextGlobalIndexReader reader = createReader(fileReader, metas)) {
            FullTextSearch search = new FullTextSearch("text", matchQuery("售货员"), 10);
            Optional<ScoredGlobalIndexResult> searchResult =
                    reader.visitFullTextSearch(search).join();
            assertThat(searchResult).isPresent();

            RoaringNavigableMap64 rowIds = searchResult.get().results();
            assertThat(rowIds.getLongCardinality()).isEqualTo(1);
            assertThat(rowIds.contains(0L)).isTrue();
        }
    }

    @Test
    public void testSearchNoResults() throws IOException {
        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        NativeFullTextGlobalIndexWriter writer = new NativeFullTextGlobalIndexWriter(fileWriter);

        writer.write(BinaryString.fromString("Hello world"), 0);
        writer.write(BinaryString.fromString("Foo bar baz"), 1);

        List<ResultEntry> results = writer.finish();
        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);
        GlobalIndexFileReader fileReader = createFileReader();

        try (NativeFullTextGlobalIndexReader reader = createReader(fileReader, metas)) {
            FullTextSearch search = new FullTextSearch("text", matchQuery("nonexistent"), 10);
            Optional<ScoredGlobalIndexResult> searchResult =
                    reader.visitFullTextSearch(search).join();
            assertThat(searchResult).isPresent();

            RoaringNavigableMap64 rowIds = searchResult.get().results();
            assertThat(rowIds.getLongCardinality()).isEqualTo(0);
        }
    }

    @Test
    public void testNullFieldSkipped() throws IOException {
        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        NativeFullTextGlobalIndexWriter writer = new NativeFullTextGlobalIndexWriter(fileWriter);

        writer.write(BinaryString.fromString("Paimon data lake"), 0);
        writer.write(null, 1); // row 1 is null, should be skipped
        writer.write(BinaryString.fromString("Paimon streaming"), 2);

        List<ResultEntry> results = writer.finish();
        assertThat(results.get(0).rowCount()).isEqualTo(3);

        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);
        GlobalIndexFileReader fileReader = createFileReader();

        try (NativeFullTextGlobalIndexReader reader = createReader(fileReader, metas)) {
            FullTextSearch search = new FullTextSearch("text", matchQuery("paimon"), 10);
            Optional<ScoredGlobalIndexResult> searchResult =
                    reader.visitFullTextSearch(search).join();
            assertThat(searchResult).isPresent();

            ScoredGlobalIndexResult scored = searchResult.get();
            RoaringNavigableMap64 rowIds = scored.results();
            // Row 0 and row 2 match, row 1 was null
            assertThat(rowIds.getLongCardinality()).isEqualTo(2);
            assertThat(rowIds.contains(0L)).isTrue();
            assertThat(rowIds.contains(1L)).isFalse();
            assertThat(rowIds.contains(2L)).isTrue();
        }
    }

    @Test
    public void testEmptyIndex() {
        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        NativeFullTextGlobalIndexWriter writer = new NativeFullTextGlobalIndexWriter(fileWriter);

        List<ResultEntry> results = writer.finish();
        assertThat(results).isEmpty();
    }

    @Test
    public void testLargeDataset() throws IOException {
        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        NativeFullTextGlobalIndexWriter writer = new NativeFullTextGlobalIndexWriter(fileWriter);

        int numDocs = 500;
        for (int i = 0; i < numDocs; i++) {
            String text = "document number " + i + " with some searchable content";
            if (i % 10 == 0) {
                text += " special_keyword";
            }
            writer.write(BinaryString.fromString(text), i);
        }

        List<ResultEntry> results = writer.finish();
        assertThat(results.get(0).rowCount()).isEqualTo(numDocs);

        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);
        GlobalIndexFileReader fileReader = createFileReader();

        try (NativeFullTextGlobalIndexReader reader = createReader(fileReader, metas)) {
            // Search for the special keyword — should match every 10th doc
            FullTextSearch search = new FullTextSearch("text", matchQuery("special_keyword"), 1000);
            Optional<ScoredGlobalIndexResult> searchResult =
                    reader.visitFullTextSearch(search).join();
            assertThat(searchResult).isPresent();

            ScoredGlobalIndexResult scored = searchResult.get();
            RoaringNavigableMap64 rowIds = scored.results();
            assertThat(rowIds.getLongCardinality()).isEqualTo(50);
            assertThat(rowIds.contains(0L)).isTrue();
            assertThat(rowIds.contains(10L)).isTrue();
            assertThat(rowIds.contains(490L)).isTrue();
            assertThat(rowIds.contains(1L)).isFalse();
        }
    }

    @Test
    public void testLimitRespected() throws IOException {
        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        NativeFullTextGlobalIndexWriter writer = new NativeFullTextGlobalIndexWriter(fileWriter);

        for (int i = 0; i < 20; i++) {
            writer.write(BinaryString.fromString("paimon document " + i), i);
        }

        List<ResultEntry> results = writer.finish();
        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);
        GlobalIndexFileReader fileReader = createFileReader();

        try (NativeFullTextGlobalIndexReader reader = createReader(fileReader, metas)) {
            // Limit to 5 results
            FullTextSearch search = new FullTextSearch("text", matchQuery("paimon"), 5);
            Optional<ScoredGlobalIndexResult> searchResult =
                    reader.visitFullTextSearch(search).join();
            assertThat(searchResult).isPresent();

            RoaringNavigableMap64 rowIds = searchResult.get().results();
            assertThat(rowIds.getLongCardinality()).isEqualTo(5);
        }
    }

    @Test
    public void testIncludeRowIdsSearchesFullShardBeforeTopK() throws IOException {
        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        NativeFullTextGlobalIndexWriter writer = new NativeFullTextGlobalIndexWriter(fileWriter);

        writer.write(BinaryString.fromString("paimon paimon paimon paimon"), 0);
        writer.write(BinaryString.fromString("paimon paimon"), 1);
        writer.write(BinaryString.fromString("paimon"), 2);

        List<ResultEntry> results = writer.finish();
        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);
        GlobalIndexFileReader fileReader = createFileReader();

        RoaringNavigableMap64 includeRowIds = new RoaringNavigableMap64();
        includeRowIds.add(2L);
        FullTextSearch search =
                new FullTextSearch("text", matchQuery("paimon"), 1)
                        .withIncludeRowIds(includeRowIds);

        try (NativeFullTextGlobalIndexReader reader = createReader(fileReader, metas)) {
            Optional<ScoredGlobalIndexResult> searchResult =
                    reader.visitFullTextSearch(search).join();
            assertThat(searchResult).isPresent();

            RoaringNavigableMap64 rowIds = searchResult.get().results();
            assertThat(rowIds.getLongCardinality()).isEqualTo(1);
            assertThat(rowIds.contains(2L)).isTrue();
        }
    }

    @Test
    public void testRepeatedReadersReturnConsistentResults() throws IOException {
        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        NativeFullTextGlobalIndexWriter writer = new NativeFullTextGlobalIndexWriter(fileWriter);
        writer.write(BinaryString.fromString("Apache Paimon streaming lake"), 0);
        writer.write(BinaryString.fromString("Native full-text search"), 1);

        List<ResultEntry> results = writer.finish();
        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);
        GlobalIndexFileReader fileReader = createFileReader();
        FullTextSearch search = new FullTextSearch("text", matchQuery("paimon"), 10);

        try (NativeFullTextGlobalIndexReader reader = createReader(fileReader, metas)) {
            Optional<ScoredGlobalIndexResult> result = reader.visitFullTextSearch(search).join();
            assertThat(result).isPresent();
            assertThat(result.get().results().contains(0L)).isTrue();
        }

        try (NativeFullTextGlobalIndexReader reader = createReader(fileReader, metas)) {
            Optional<ScoredGlobalIndexResult> result = reader.visitFullTextSearch(search).join();
            assertThat(result).isPresent();
            assertThat(result.get().results().getLongCardinality()).isEqualTo(1);
            assertThat(result.get().results().contains(0L)).isTrue();
        }
    }

    @Test
    public void testViaIndexer() throws IOException {
        NativeFullTextGlobalIndexer indexer = new NativeFullTextGlobalIndexer();

        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        NativeFullTextGlobalIndexWriter writer =
                (NativeFullTextGlobalIndexWriter) indexer.createWriter(fileWriter);

        writer.write(BinaryString.fromString("test via indexer factory"), 0);
        List<ResultEntry> results = writer.finish();
        assertThat(results).hasSize(1);

        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);
        GlobalIndexFileReader fileReader = createFileReader();

        try (NativeFullTextGlobalIndexReader reader =
                (NativeFullTextGlobalIndexReader)
                        indexer.createReader(fileReader, metas, newDirectExecutorService())) {
            FullTextSearch search = new FullTextSearch("text", matchQuery("indexer"), 10);
            Optional<ScoredGlobalIndexResult> searchResult =
                    reader.visitFullTextSearch(search).join();
            assertThat(searchResult).isPresent();
            assertThat(searchResult.get().results().getLongCardinality()).isEqualTo(1);
            assertThat(searchResult.get().results().contains(0L)).isTrue();
        }
    }

    @Test
    public void testStructuredBoostQueryDemotesNegativeMatches() throws IOException {
        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        NativeFullTextGlobalIndexWriter writer = new NativeFullTextGlobalIndexWriter(fileWriter);

        writer.write(BinaryString.fromString("paimon lake"), 0);
        writer.write(BinaryString.fromString("paimon vector"), 1);

        List<ResultEntry> results = writer.finish();
        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);
        GlobalIndexFileReader fileReader = createFileReader();

        try (NativeFullTextGlobalIndexReader reader = createReader(fileReader, metas)) {
            FullTextSearch search =
                    new FullTextSearch("text", boostQuery("paimon", "vector", 0.5f), 10);
            Optional<ScoredGlobalIndexResult> searchResult =
                    reader.visitFullTextSearch(search).join();
            assertThat(searchResult).isPresent();
            assertThat(searchResult.get().results().contains(0L)).isTrue();
            assertThat(searchResult.get().results().contains(1L)).isTrue();
            assertThat(searchResult.get().scoreGetter().score(0L))
                    .isGreaterThan(searchResult.get().scoreGetter().score(1L));
        }
    }

    private static String matchQuery(String terms) {
        return "{\"match\":{\"query\":\"" + terms + "\"}}";
    }

    private static String boostQuery(String positive, String negative, float negativeBoost) {
        return "{\"boost\":{\"positive\":"
                + matchQuery(positive)
                + ",\"negative\":"
                + matchQuery(negative)
                + ",\"negative_boost\":"
                + negativeBoost
                + "}}";
    }
}
