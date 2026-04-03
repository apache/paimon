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
import org.apache.paimon.predicate.FullTextSearch;
import org.apache.paimon.tantivy.NativeLoader;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Test for {@link TantivyFullTextGlobalIndexWriter} and {@link TantivyFullTextGlobalIndexReader}.
 */
public class TantivyFullTextGlobalIndexTest {

    @BeforeAll
    static void checkNativeLibrary() {
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

    @TempDir java.nio.file.Path tempDir;

    private FileIO fileIO;
    private Path indexPath;

    @BeforeEach
    public void setup() {
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

    @Test
    public void testEndToEnd() throws IOException {
        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        TantivyFullTextGlobalIndexWriter writer = new TantivyFullTextGlobalIndexWriter(fileWriter);

        writer.write(BinaryString.fromString("Apache Paimon is a streaming data lake platform"));
        writer.write(BinaryString.fromString("Tantivy is a full-text search engine in Rust"));
        writer.write(BinaryString.fromString("Paimon supports real-time data ingestion"));

        List<ResultEntry> results = writer.finish();
        assertThat(results).hasSize(1);
        assertThat(results.get(0).rowCount()).isEqualTo(3);

        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);
        GlobalIndexFileReader fileReader = createFileReader();

        try (TantivyFullTextGlobalIndexReader reader =
                new TantivyFullTextGlobalIndexReader(fileReader, metas)) {
            FullTextSearch search = new FullTextSearch("paimon", 10, "text");
            Optional<ScoredGlobalIndexResult> searchResult = reader.visitFullTextSearch(search);
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
    public void testSearchNoResults() throws IOException {
        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        TantivyFullTextGlobalIndexWriter writer = new TantivyFullTextGlobalIndexWriter(fileWriter);

        writer.write(BinaryString.fromString("Hello world"));
        writer.write(BinaryString.fromString("Foo bar baz"));

        List<ResultEntry> results = writer.finish();
        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);
        GlobalIndexFileReader fileReader = createFileReader();

        try (TantivyFullTextGlobalIndexReader reader =
                new TantivyFullTextGlobalIndexReader(fileReader, metas)) {
            FullTextSearch search = new FullTextSearch("nonexistent", 10, "text");
            Optional<ScoredGlobalIndexResult> searchResult = reader.visitFullTextSearch(search);
            assertThat(searchResult).isPresent();

            RoaringNavigableMap64 rowIds = searchResult.get().results();
            assertThat(rowIds.getLongCardinality()).isEqualTo(0);
        }
    }

    @Test
    public void testNullFieldSkipped() throws IOException {
        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        TantivyFullTextGlobalIndexWriter writer = new TantivyFullTextGlobalIndexWriter(fileWriter);

        writer.write(BinaryString.fromString("Paimon data lake"));
        writer.write(null); // row 1 is null, should be skipped
        writer.write(BinaryString.fromString("Paimon streaming"));

        List<ResultEntry> results = writer.finish();
        assertThat(results.get(0).rowCount()).isEqualTo(3);

        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);
        GlobalIndexFileReader fileReader = createFileReader();

        try (TantivyFullTextGlobalIndexReader reader =
                new TantivyFullTextGlobalIndexReader(fileReader, metas)) {
            FullTextSearch search = new FullTextSearch("paimon", 10, "text");
            Optional<ScoredGlobalIndexResult> searchResult = reader.visitFullTextSearch(search);
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
        TantivyFullTextGlobalIndexWriter writer = new TantivyFullTextGlobalIndexWriter(fileWriter);

        List<ResultEntry> results = writer.finish();
        assertThat(results).isEmpty();
    }

    @Test
    public void testLargeDataset() throws IOException {
        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        TantivyFullTextGlobalIndexWriter writer = new TantivyFullTextGlobalIndexWriter(fileWriter);

        int numDocs = 500;
        for (int i = 0; i < numDocs; i++) {
            String text = "document number " + i + " with some searchable content";
            if (i % 10 == 0) {
                text += " special_keyword";
            }
            writer.write(BinaryString.fromString(text));
        }

        List<ResultEntry> results = writer.finish();
        assertThat(results.get(0).rowCount()).isEqualTo(numDocs);

        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);
        GlobalIndexFileReader fileReader = createFileReader();

        try (TantivyFullTextGlobalIndexReader reader =
                new TantivyFullTextGlobalIndexReader(fileReader, metas)) {
            // Search for the special keyword — should match every 10th doc
            FullTextSearch search = new FullTextSearch("special_keyword", 1000, "text");
            Optional<ScoredGlobalIndexResult> searchResult = reader.visitFullTextSearch(search);
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
        TantivyFullTextGlobalIndexWriter writer = new TantivyFullTextGlobalIndexWriter(fileWriter);

        for (int i = 0; i < 20; i++) {
            writer.write(BinaryString.fromString("paimon document " + i));
        }

        List<ResultEntry> results = writer.finish();
        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);
        GlobalIndexFileReader fileReader = createFileReader();

        try (TantivyFullTextGlobalIndexReader reader =
                new TantivyFullTextGlobalIndexReader(fileReader, metas)) {
            // Limit to 5 results
            FullTextSearch search = new FullTextSearch("paimon", 5, "text");
            Optional<ScoredGlobalIndexResult> searchResult = reader.visitFullTextSearch(search);
            assertThat(searchResult).isPresent();

            RoaringNavigableMap64 rowIds = searchResult.get().results();
            assertThat(rowIds.getLongCardinality()).isEqualTo(5);
        }
    }

    @Test
    public void testViaIndexer() throws IOException {
        TantivyFullTextGlobalIndexer indexer = new TantivyFullTextGlobalIndexer();

        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        TantivyFullTextGlobalIndexWriter writer =
                (TantivyFullTextGlobalIndexWriter) indexer.createWriter(fileWriter);

        writer.write(BinaryString.fromString("test via indexer factory"));
        List<ResultEntry> results = writer.finish();
        assertThat(results).hasSize(1);

        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);
        GlobalIndexFileReader fileReader = createFileReader();

        try (TantivyFullTextGlobalIndexReader reader =
                (TantivyFullTextGlobalIndexReader) indexer.createReader(fileReader, metas)) {
            FullTextSearch search = new FullTextSearch("indexer", 10, "text");
            Optional<ScoredGlobalIndexResult> searchResult = reader.visitFullTextSearch(search);
            assertThat(searchResult).isPresent();
            assertThat(searchResult.get().results().getLongCardinality()).isEqualTo(1);
            assertThat(searchResult.get().results().contains(0L)).isTrue();
        }
    }
}
