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

package org.apache.paimon.eslib.index;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.SeekableInputStreamWrapper;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FullTextSearch;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end test of the P5 ↔ P4 integration: {@link ESIndexGlobalIndexWriter} feeds Paimon rows to
 * {@code DefaultESIndexBuilder} (loaded reflectively), packs the Lucene segment files into an
 * archive, and {@link ESIndexGlobalIndexReader} reads that archive back through {@code
 * DefaultESIndexSearcher} (also reflective) to serve vector and full-text queries.
 *
 * <p>This exercises the real archive write/offset-table/read roundtrip and the eslib-core
 * reflection loading, without Flink or OSS.
 */
class ESIndexGlobalIndexE2ETest {

    /** {@link GlobalIndexFileWriter} backed by a local directory. */
    private static final class LocalDirWriter implements GlobalIndexFileWriter {
        private final java.nio.file.Path dir;
        private final LocalFileIO fio = LocalFileIO.create();

        LocalDirWriter(java.nio.file.Path dir) {
            this.dir = dir;
        }

        @Override
        public String newFileName(String prefix) {
            return prefix + "-" + UUID.randomUUID() + ".index";
        }

        @Override
        public PositionOutputStream newOutputStream(String fileName) throws IOException {
            return fio.newOutputStream(
                    new org.apache.paimon.fs.Path(dir.resolve(fileName).toString()), true);
        }
    }

    /** {@link GlobalIndexFileReader} backed by local files. */
    private static final class LocalFileReader implements GlobalIndexFileReader {
        private final LocalFileIO fio = LocalFileIO.create();

        @Override
        public SeekableInputStream getInputStream(GlobalIndexIOMeta meta) throws IOException {
            return fio.newInputStream(meta.filePath());
        }
    }

    /** Blocks the first lazy-load stream open and counts every stream's lifecycle. */
    private static final class BlockingCountingFileReader implements GlobalIndexFileReader {
        private final LocalFileIO fio = LocalFileIO.create();
        private final CountDownLatch firstOpenStarted = new CountDownLatch(1);
        private final CountDownLatch allowOpen = new CountDownLatch(1);
        private final AtomicInteger openCount = new AtomicInteger();
        private final AtomicInteger closeCount = new AtomicInteger();

        @Override
        public SeekableInputStream getInputStream(GlobalIndexIOMeta meta) throws IOException {
            firstOpenStarted.countDown();
            try {
                allowOpen.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting to open index stream", e);
            }

            openCount.incrementAndGet();
            return new SeekableInputStreamWrapper(fio.newInputStream(meta.filePath())) {
                private final AtomicBoolean streamClosed = new AtomicBoolean();

                @Override
                public void close() throws IOException {
                    if (streamClosed.compareAndSet(false, true)) {
                        try {
                            super.close();
                        } finally {
                            closeCount.incrementAndGet();
                        }
                    }
                }
            };
        }

        boolean awaitFirstOpen() throws InterruptedException {
            return firstOpenStarted.await(10, TimeUnit.SECONDS);
        }

        void releaseOpen() {
            allowOpen.countDown();
        }

        int openCount() {
            return openCount.get();
        }

        int closeCount() {
            return closeCount.get();
        }
    }

    @Test
    void writeArchiveThenReadAndSearch(@TempDir java.nio.file.Path tmp) throws IOException {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "embedding", DataTypes.ARRAY(DataTypes.FLOAT())),
                        new DataField(1, "title", DataTypes.STRING()),
                        new DataField(2, "category", DataTypes.STRING()),
                        new DataField(3, "price", DataTypes.INT()));

        Map<String, String> opt = new HashMap<>();
        opt.put("global-index.es-index.fields.embedding.algorithm", "hnsw");
        opt.put("global-index.es-index.fields.embedding.dimension", "4");
        opt.put("global-index.es-index.fields.embedding.metric", "l2");
        opt.put("global-index.es-index.fields.title.analyzer", "standard");
        // category uses KEYWORD as its primary field; ESIndexOptions adds category.fulltext so the
        // same logical field can also serve analyzed search.
        opt.put("global-index.es-index.fields.category.type", "keyword");
        ESIndexOptions options = new ESIndexOptions(fields, Options.fromMap(opt));

        java.nio.file.Path archiveDir = tmp.resolve("archive");
        Files.createDirectories(archiveDir);

        // --- write 20 rows → archive ---
        LocalDirWriter fileWriter = new LocalDirWriter(archiveDir);
        ESIndexGlobalIndexWriter writer = new ESIndexGlobalIndexWriter(fileWriter, fields, options);
        for (int i = 0; i < 20; i++) {
            GenericRow row =
                    GenericRow.of(
                            new GenericArray(new float[] {i, i, i, i}),
                            BinaryString.fromString(
                                    "document " + i + (i % 2 == 0 ? " even" : " odd")),
                            BinaryString.fromString(i % 2 == 0 ? "even" : "odd"),
                            i * 10);
            writer.write(i, row);
        }
        List<ResultEntry> entries = writer.finish();
        assertEquals(1, entries.size(), "one archive produced");
        ResultEntry entry = entries.get(0);
        assertEquals(20L, entry.rowCount(), "row count");
        assertNotNull(entry.meta(), "offset-table meta present");

        // --- reconstruct reader over the archive ---
        org.apache.paimon.fs.Path filePath =
                new org.apache.paimon.fs.Path(archiveDir.resolve(entry.fileName()).toString());
        long fileSize = Files.size(archiveDir.resolve(entry.fileName()));
        GlobalIndexIOMeta ioMeta = new GlobalIndexIOMeta(filePath, fileSize, entry.meta());

        ESIndexGlobalIndexReader reader =
                new ESIndexGlobalIndexReader(
                        new LocalFileReader(), List.of(ioMeta), fields, options);

        // --- vector search: nearest to origin is row 0 (vector [0,0,0,0]) ---
        Optional<ScoredGlobalIndexResult> vr =
                reader.visitVectorSearch(new VectorSearch(new float[] {0, 0, 0, 0}, 5, "embedding"))
                        .join();
        assertTrue(vr.isPresent(), "vector search returns a result");
        RoaringNavigableMap64 vrows = vr.get().results();
        assertEquals(5, vrows.getIntCardinality(), "k=5 results");
        assertTrue(contains(vrows, 0L), "row 0 (origin) recalled by HNSW");

        // --- full-text search: 10 rows contain "even" ---
        Optional<ScoredGlobalIndexResult> ft =
                reader.visitFullTextSearch(new FullTextSearch("title", matchQuery("even"), 50))
                        .join();
        assertTrue(ft.isPresent(), "full-text search returns a result");
        assertEquals(10, ft.get().results().getIntCardinality(), "10 even docs");

        // --- Phrase query honours token order: titles are "document {i} even/odd". "document 0"
        // is a consecutive phrase only in row 0; "even document" never occurs in that order. ---
        Optional<ScoredGlobalIndexResult> phraseHit =
                reader.visitFullTextSearch(
                                new FullTextSearch("title", phraseQuery("document 0"), 50))
                        .join();
        assertTrue(phraseHit.isPresent(), "phrase 'document 0' matches");
        assertEquals(
                1, phraseHit.get().results().getIntCardinality(), "only row 0 has 'document 0'");
        assertTrue(contains(phraseHit.get().results(), 0L), "phrase 'document 0' is row 0");
        assertTrue(
                reader.visitFullTextSearch(
                                new FullTextSearch("title", phraseQuery("even document"), 50))
                        .join()
                        .isEmpty(),
                "phrase 'even document' never occurs in that order");

        // --- operator is honoured: every doc has "document", only 10 have "even" ---
        // default OR("even document") matches all 20; AND("even document") matches only the 10
        // even docs that contain both tokens.
        Optional<ScoredGlobalIndexResult> orFt =
                reader.visitFullTextSearch(
                                new FullTextSearch("title", matchQuery("even document"), 50))
                        .join();
        assertEquals(
                20,
                orFt.get().results().getIntCardinality(),
                "OR match on 'even document' hits every doc (all contain 'document')");
        Optional<ScoredGlobalIndexResult> andFt =
                reader.visitFullTextSearch(
                                new FullTextSearch("title", matchQuery("even document", "and"), 50))
                        .join();
        assertEquals(
                10,
                andFt.get().results().getIntCardinality(),
                "AND match on 'even document' hits only docs with both tokens");

        // --- fuzziness is honoured: "evon" is one edit from "even" ---
        Optional<ScoredGlobalIndexResult> exactTypo =
                reader.visitFullTextSearch(new FullTextSearch("title", matchQuery("evon"), 50))
                        .join();
        assertTrue(exactTypo.isEmpty(), "exact 'evon' matches nothing");
        Optional<ScoredGlobalIndexResult> fuzzyTypo =
                reader.visitFullTextSearch(
                                new FullTextSearch("title", fuzzyMatchQuery("evon", 1), 50))
                        .join();
        assertEquals(
                10,
                fuzzyTypo.get().results().getIntCardinality(),
                "fuzzy 'evon'~1 matches the 10 'even' docs");

        // --- category is KEYWORD-primary, but full-text search routes to category.fulltext. ---
        Optional<ScoredGlobalIndexResult> categoryFt =
                reader.visitFullTextSearch(new FullTextSearch("category", matchQuery("even"), 50))
                        .join();
        assertTrue(categoryFt.isPresent(), "KEYWORD field has a FULLTEXT multi-field");
        assertEquals(
                10,
                categoryFt.get().results().getIntCardinality(),
                "category.fulltext matches 10 even rows");

        // --- multi-field: title is FULLTEXT, so ordinary predicates route to the keyword
        // sub-field title.keyword (written by default) and evaluate exactly on the raw value ---
        FieldRef titleRef = new FieldRef(1, "title", DataTypes.STRING());
        Optional<GlobalIndexResult> titleEq = reader.visitEqual(titleRef, "document 0 even").join();
        assertTrue(titleEq.isPresent(), "= on FULLTEXT routes to keyword sub-field");
        assertEquals(
                1, titleEq.get().results().getIntCardinality(), "exact title matches only row 0");
        assertTrue(contains(titleEq.get().results(), 0L), "exact title 'document 0 even' is row 0");
        assertEquals(
                20,
                reader.visitLike(titleRef, "doc%").join().get().results().getIntCardinality(),
                "LIKE 'doc%' on title.keyword matches all 20 'document ...' titles");
        // A range predicate on a string keyword sub-field is unsupported -> falls back to empty
        // (not an error).
        assertTrue(
                reader.visitGreaterThan(titleRef, "abc").join().isEmpty(),
                "> (range) on a keyword sub-field is unsupported and falls back to empty");

        // --- scalar filter: price >= 100 means rows 10..19 (price = i*10) ---
        FieldRef priceRef = new FieldRef(3, "price", DataTypes.INT());
        Optional<GlobalIndexResult> sf = reader.visitGreaterOrEqual(priceRef, 100).join();
        assertTrue(sf.isPresent(), "scalar filter returns a result");
        assertEquals(10, sf.get().results().getIntCardinality(), "10 rows with price >= 100");

        // --- scalar filter: price == 50 → row 5 only ---
        Optional<GlobalIndexResult> eq = reader.visitEqual(priceRef, 50).join();
        assertTrue(eq.isPresent(), "scalar eq filter returns a result");
        assertEquals(1, eq.get().results().getIntCardinality(), "1 row with price == 50");
        assertTrue(contains(eq.get().results(), 5L), "row 5 has price=50");

        // --- scalar negative filters are implemented in the reader as exists AND NOT(eq/in). ---
        Optional<GlobalIndexResult> ne = reader.visitNotEqual(priceRef, 50).join();
        assertTrue(ne.isPresent(), "scalar <> filter returns a result");
        assertEquals(19, ne.get().results().getIntCardinality(), "all rows except price=50");
        assertTrue(!contains(ne.get().results(), 5L), "row 5 must be excluded by price<>50");

        Optional<GlobalIndexResult> notIn =
                reader.visitNotIn(priceRef, Arrays.asList(50, 60)).join();
        assertTrue(notIn.isPresent(), "scalar NOT IN filter returns a result");
        assertEquals(
                18, notIn.get().results().getIntCardinality(), "all rows except price in (50, 60)");
        assertTrue(!contains(notIn.get().results(), 5L) && !contains(notIn.get().results(), 6L));

        // --- keyword filter: category == "even" → 10 rows ---
        FieldRef categoryRef = new FieldRef(2, "category", DataTypes.STRING());
        Optional<GlobalIndexResult> kw = reader.visitEqual(categoryRef, "even").join();
        assertTrue(kw.isPresent(), "keyword eq filter returns a result");
        assertEquals(10, kw.get().results().getIntCardinality(), "10 even category rows");

        // --- keyword prefix: category startsWith "ev" → 10 rows with "even" ---
        Optional<GlobalIndexResult> sw = reader.visitStartsWith(categoryRef, "ev").join();
        assertTrue(sw.isPresent(), "startsWith filter returns a result");
        assertEquals(10, sw.get().results().getIntCardinality(), "10 rows start with 'ev'");

        // --- keyword contains: category contains "dd" → 10 rows with "odd" ---
        Optional<GlobalIndexResult> ct = reader.visitContains(categoryRef, "dd").join();
        assertTrue(ct.isPresent(), "contains filter returns a result");
        assertEquals(10, ct.get().results().getIntCardinality(), "10 rows contain 'dd'");

        try {
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void textMultiFieldServesBothMatchAndExact(@TempDir java.nio.file.Path tmp) throws IOException {
        // Whichever text representation is primary, the complementary multi-field is written so
        // both analyzed search and exact predicates are always available.
        List<DataField> fields = Arrays.asList(new DataField(0, "t", DataTypes.STRING()));
        String[] docs = {"Apache Paimon", "vector search", "Apache Paimon"};

        for (String primaryType : new String[] {"fulltext", "keyword"}) {
            Map<String, String> opt = new HashMap<>();
            opt.put("global-index.es-index.fields.t.type", primaryType);
            ESIndexOptions options = new ESIndexOptions(fields, Options.fromMap(opt));

            java.nio.file.Path dir = tmp.resolve("mf-" + primaryType);
            Files.createDirectories(dir);
            LocalDirWriter fw = new LocalDirWriter(dir);
            ESIndexGlobalIndexWriter w = new ESIndexGlobalIndexWriter(fw, fields, options);
            for (int i = 0; i < docs.length; i++) {
                w.write(BinaryString.fromString(docs[i]), i);
            }
            ResultEntry entry = w.finish().get(0);
            GlobalIndexIOMeta ioMeta =
                    new GlobalIndexIOMeta(
                            new org.apache.paimon.fs.Path(dir.resolve(entry.fileName()).toString()),
                            Files.size(dir.resolve(entry.fileName())),
                            entry.meta());
            ESIndexGlobalIndexReader reader =
                    new ESIndexGlobalIndexReader(
                            new LocalFileReader(), List.of(ioMeta), fields, options);
            FieldRef tRef = new FieldRef(0, "t", DataTypes.STRING());

            // FULLTEXT-primary searches t; KEYWORD-primary searches t.fulltext.
            Optional<ScoredGlobalIndexResult> m =
                    reader.visitFullTextSearch(new FullTextSearch("t", matchQuery("paimon"), 50))
                            .join();
            assertTrue(m.isPresent(), "match works for " + primaryType + " primary");
            assertEquals(2, m.get().results().getIntCardinality(), "two docs contain 'paimon'");

            // FULLTEXT-primary routes exact matching to t.keyword; KEYWORD-primary uses t.
            Optional<GlobalIndexResult> eq = reader.visitEqual(tRef, "Apache Paimon").join();
            assertTrue(eq.isPresent(), "exact = works for " + primaryType + " primary");
            assertEquals(
                    2, eq.get().results().getIntCardinality(), "two rows equal 'Apache Paimon'");
            try {
                reader.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    void arrayLongLabelsRoundTripAsMultiValueScalar(@TempDir java.nio.file.Path tmp)
            throws IOException {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "id", DataTypes.BIGINT()),
                        new DataField(1, "labels", DataTypes.ARRAY(DataTypes.BIGINT())));
        ESIndexOptions options = new ESIndexOptions(fields, Options.fromMap(new HashMap<>()));

        java.nio.file.Path archiveDir = tmp.resolve("archive-labels");
        Files.createDirectories(archiveDir);

        LocalDirWriter fileWriter = new LocalDirWriter(archiveDir);
        ESIndexGlobalIndexWriter writer = new ESIndexGlobalIndexWriter(fileWriter, fields, options);
        writer.write(0, GenericRow.of(100L, new GenericArray(new long[] {3L, 7L})));
        writer.write(1, GenericRow.of(101L, new GenericArray(new long[] {5L})));
        writer.write(2, GenericRow.of(102L, new GenericArray(new long[] {7L, 11L})));

        List<ResultEntry> entries = writer.finish();
        assertEquals(1, entries.size(), "one archive produced");
        ResultEntry entry = entries.get(0);
        assertEquals(3L, entry.rowCount(), "row count");

        org.apache.paimon.fs.Path filePath =
                new org.apache.paimon.fs.Path(archiveDir.resolve(entry.fileName()).toString());
        long fileSize = Files.size(archiveDir.resolve(entry.fileName()));
        GlobalIndexIOMeta ioMeta = new GlobalIndexIOMeta(filePath, fileSize, entry.meta());
        ESIndexGlobalIndexReader reader =
                new ESIndexGlobalIndexReader(
                        new LocalFileReader(), List.of(ioMeta), fields, options);

        FieldRef labelsRef = new FieldRef(1, "labels", DataTypes.ARRAY(DataTypes.BIGINT()));

        Optional<GlobalIndexResult> hit7 = reader.visitEqual(labelsRef, 7L).join();
        assertTrue(hit7.isPresent(), "labels term filter returns a result");
        RoaringNavigableMap64 rows7 = hit7.get().results();
        assertEquals(2, rows7.getIntCardinality(), "two rows contain label 7");
        assertTrue(contains(rows7, 0L), "row 0 contains label 7");
        assertTrue(contains(rows7, 2L), "row 2 contains label 7");

        Optional<GlobalIndexResult> hitIn =
                reader.visitIn(labelsRef, Arrays.asList(5L, 11L)).join();
        assertTrue(hitIn.isPresent(), "labels IN filter returns a result");
        RoaringNavigableMap64 rowsIn = hitIn.get().results();
        assertEquals(2, rowsIn.getIntCardinality(), "two rows contain labels in (5, 11)");
        assertTrue(contains(rowsIn, 1L), "row 1 contains label 5");
        assertTrue(contains(rowsIn, 2L), "row 2 contains label 11");

        try {
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void nullValuesKeepDocIdRowIdAligned(@TempDir java.nio.file.Path tmp) throws IOException {
        // Single-column keyword index where row 3's value is NULL. The writer skips null and
        // registers
        // an empty doc for that slot (addNullDoc), with flushPendingDocs as the back-stop; both
        // stamp
        // _ROW_ID=docId so the null gap is occupied. This guards docId<->rowId alignment: an exact
        // term lookup must resolve to the row's OWN id. A regression in padding / _ROW_ID stamping
        // would shift every row after the gap by one (e.g. "row7" would come back as rowId 6).
        List<DataField> fields = Arrays.asList(new DataField(0, "k", DataTypes.STRING()));
        // No analyzer on "k" -> KEYWORD (exact-term) index.
        ESIndexOptions options = new ESIndexOptions(fields, Options.fromMap(new HashMap<>()));

        java.nio.file.Path archiveDir = tmp.resolve("archive-null");
        Files.createDirectories(archiveDir);
        LocalDirWriter fileWriter = new LocalDirWriter(archiveDir);
        ESIndexGlobalIndexWriter writer = new ESIndexGlobalIndexWriter(fileWriter, fields, options);

        int n = 10;
        int nullRow = 3;
        for (int i = 0; i < n; i++) {
            writer.write(i == nullRow ? null : BinaryString.fromString("row" + i), i);
        }
        List<ResultEntry> entries = writer.finish();
        assertEquals(1, entries.size(), "one archive produced");
        ResultEntry entry = entries.get(0);
        assertEquals((long) n, entry.rowCount(), "row count includes the null row");

        org.apache.paimon.fs.Path filePath =
                new org.apache.paimon.fs.Path(archiveDir.resolve(entry.fileName()).toString());
        long fileSize = Files.size(archiveDir.resolve(entry.fileName()));
        GlobalIndexIOMeta ioMeta = new GlobalIndexIOMeta(filePath, fileSize, entry.meta());
        ESIndexGlobalIndexReader reader =
                new ESIndexGlobalIndexReader(
                        new LocalFileReader(), List.of(ioMeta), fields, options);

        FieldRef kRef = new FieldRef(0, "k", DataTypes.STRING());
        for (int i = 0; i < n; i++) {
            if (i == nullRow) {
                continue;
            }
            Optional<GlobalIndexResult> r = reader.visitEqual(kRef, "row" + i).join();
            assertTrue(r.isPresent(), "term lookup returns for row" + i);
            RoaringNavigableMap64 rows = r.get().results();
            assertEquals(1, rows.getIntCardinality(), "exactly one match for row" + i);
            assertTrue(contains(rows, (long) i), "term 'row" + i + "' must map to rowId " + i);
        }

        try {
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void likeHonorsSqlEscapeAndLuceneLiterals(@TempDir java.nio.file.Path tmp) throws IOException {
        // Paimon's default SQL LIKE escape is '\': \_ and \% are literals, while unescaped '%' /
        // '_'
        // are wildcards. '*' / '?' are ordinary SQL characters and must also be escaped before the
        // pattern reaches Lucene's WildcardQuery.
        List<DataField> fields = Arrays.asList(new DataField(0, "k", DataTypes.STRING()));
        Map<String, String> optionMap = new HashMap<>();
        optionMap.put("global-index.es-index.fields.k.type", "keyword");
        ESIndexOptions options = new ESIndexOptions(fields, Options.fromMap(optionMap));

        java.nio.file.Path archiveDir = tmp.resolve("archive-like");
        Files.createDirectories(archiveDir);
        LocalDirWriter fileWriter = new LocalDirWriter(archiveDir);
        ESIndexGlobalIndexWriter writer = new ESIndexGlobalIndexWriter(fileWriter, fields, options);

        // Lucene metacharacter cases plus SQL backslash-escape cases.
        writer.write(BinaryString.fromString("abc"), 0);
        writer.write(BinaryString.fromString("a*c"), 1);
        writer.write(BinaryString.fromString("axc"), 2);
        writer.write(BinaryString.fromString("admin_001"), 3);
        writer.write(BinaryString.fromString("adminX001"), 4);
        writer.write(BinaryString.fromString("admin%done"), 5);
        writer.write(BinaryString.fromString("admin\\path"), 6);
        List<ResultEntry> entries = writer.finish();
        ResultEntry entry = entries.get(0);

        org.apache.paimon.fs.Path filePath =
                new org.apache.paimon.fs.Path(archiveDir.resolve(entry.fileName()).toString());
        long fileSize = Files.size(archiveDir.resolve(entry.fileName()));
        GlobalIndexIOMeta ioMeta = new GlobalIndexIOMeta(filePath, fileSize, entry.meta());
        ESIndexGlobalIndexReader reader =
                new ESIndexGlobalIndexReader(
                        new LocalFileReader(), List.of(ioMeta), fields, options);

        FieldRef kRef = new FieldRef(0, "k", DataTypes.STRING());

        // LIKE 'a*c': '*' is literal -> only the exact "a*c" (row 1).
        RoaringNavigableMap64 starHits = reader.visitLike(kRef, "a*c").join().get().results();
        assertEquals(1, starHits.getIntCardinality(), "LIKE 'a*c' matches only the literal star");
        assertTrue(contains(starHits, 1L), "LIKE 'a*c' matches row1 \"a*c\"");

        // LIKE 'a_c': '_' = exactly one char -> all three 3-char a?c values.
        RoaringNavigableMap64 underHits = reader.visitLike(kRef, "a_c").join().get().results();
        assertEquals(3, underHits.getIntCardinality(), "LIKE 'a_c' matches all a?c rows");

        // LIKE 'a%c': '%' = any -> all three.
        RoaringNavigableMap64 pctHits = reader.visitLike(kRef, "a%c").join().get().results();
        assertEquals(3, pctHits.getIntCardinality(), "LIKE 'a%c' matches all a*c rows");

        // LIKE 'admin\_%': '\_' is a literal underscore and '%' remains a wildcard.
        RoaringNavigableMap64 escapedUnderHits =
                reader.visitLike(kRef, "admin\\_%").join().get().results();
        assertEquals(
                1,
                escapedUnderHits.getIntCardinality(),
                "escaped underscore matches only the literal underscore");
        assertTrue(contains(escapedUnderHits, 3L), "escaped underscore matches admin_001");

        // LIKE 'admin\%%': '\%' is a literal percent followed by a wildcard percent.
        RoaringNavigableMap64 escapedPctHits =
                reader.visitLike(kRef, "admin\\%%").join().get().results();
        assertEquals(
                1,
                escapedPctHits.getIntCardinality(),
                "escaped percent matches only the literal percent");
        assertTrue(contains(escapedPctHits, 5L), "escaped percent matches admin%done");

        // LIKE 'admin\\%': '\\' is one literal backslash followed by a wildcard percent.
        RoaringNavigableMap64 escapedSlashHits =
                reader.visitLike(kRef, "admin\\\\%").join().get().results();
        assertEquals(
                1,
                escapedSlashHits.getIntCardinality(),
                "escaped backslash matches one literal backslash");
        assertTrue(contains(escapedSlashHits, 6L), "escaped backslash matches admin\\path");

        try {
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void closeWaitsForInFlightLoadAndClosesEveryStream(@TempDir java.nio.file.Path tmp)
            throws Exception {
        List<DataField> fields = Arrays.asList(new DataField(0, "k", DataTypes.STRING()));
        Map<String, String> optionMap = new HashMap<>();
        optionMap.put("global-index.es-index.fields.k.type", "keyword");
        ESIndexOptions options = new ESIndexOptions(fields, Options.fromMap(optionMap));

        java.nio.file.Path archiveDir = tmp.resolve("archive-concurrent-close");
        Files.createDirectories(archiveDir);
        ESIndexGlobalIndexWriter writer =
                new ESIndexGlobalIndexWriter(new LocalDirWriter(archiveDir), fields, options);
        writer.write(BinaryString.fromString("admin_001"), 0);
        ResultEntry entry = writer.finish().get(0);

        org.apache.paimon.fs.Path filePath =
                new org.apache.paimon.fs.Path(archiveDir.resolve(entry.fileName()).toString());
        GlobalIndexIOMeta ioMeta =
                new GlobalIndexIOMeta(
                        filePath, Files.size(archiveDir.resolve(entry.fileName())), entry.meta());
        BlockingCountingFileReader fileReader = new BlockingCountingFileReader();
        ExecutorService searchExecutor = Executors.newFixedThreadPool(4);
        ExecutorService closeExecutor = Executors.newSingleThreadExecutor();
        ESIndexGlobalIndexReader reader =
                new ESIndexGlobalIndexReader(
                        fileReader, List.of(ioMeta), fields, options, searchExecutor);

        try {
            FieldRef kRef = new FieldRef(0, "k", DataTypes.STRING());
            CompletableFuture<Optional<GlobalIndexResult>> searchFuture =
                    reader.visitEqual(kRef, "admin_001");
            assertTrue(fileReader.awaitFirstOpen(), "lazy load reached the blocking stream open");

            CountDownLatch closeStarted = new CountDownLatch(1);
            Future<?> closeFuture =
                    closeExecutor.submit(
                            () -> {
                                closeStarted.countDown();
                                reader.close();
                                return null;
                            });
            assertTrue(closeStarted.await(10, TimeUnit.SECONDS), "close task started");
            assertThrows(
                    TimeoutException.class,
                    () -> closeFuture.get(200, TimeUnit.MILLISECONDS),
                    "close must wait for the in-flight load/search operation");

            fileReader.releaseOpen();
            Optional<GlobalIndexResult> result = searchFuture.get(10, TimeUnit.SECONDS);
            assertTrue(result.isPresent(), "in-flight search completes before close");
            assertEquals(1, result.get().results().getIntCardinality(), "one exact match");
            closeFuture.get(10, TimeUnit.SECONDS);
            assertEquals(
                    fileReader.openCount(),
                    fileReader.closeCount(),
                    "close releases every stream opened during lazy loading");
        } finally {
            fileReader.releaseOpen();
            reader.close();
            searchExecutor.shutdownNow();
            closeExecutor.shutdownNow();
        }
    }

    @Test
    void isNullReturnsNullRowsViaIndex(@TempDir java.nio.file.Path tmp) throws IOException {
        // Rows 3 and 7 have a NULL value. addNullDoc writes an empty doc (field absent) for them,
        // so
        // IS NULL must be index-evaluable (present, NOT Optional.empty()) and return exactly {3,7};
        // IS NOT NULL returns the other 8.
        List<DataField> fields = Arrays.asList(new DataField(0, "k", DataTypes.STRING()));
        ESIndexOptions options = new ESIndexOptions(fields, Options.fromMap(new HashMap<>()));

        java.nio.file.Path archiveDir = tmp.resolve("archive-isnull");
        Files.createDirectories(archiveDir);
        LocalDirWriter fileWriter = new LocalDirWriter(archiveDir);
        ESIndexGlobalIndexWriter writer = new ESIndexGlobalIndexWriter(fileWriter, fields, options);

        int n = 10;
        Set<Integer> nullRows = new HashSet<>(Arrays.asList(3, 7));
        for (int i = 0; i < n; i++) {
            writer.write(nullRows.contains(i) ? null : BinaryString.fromString("row" + i), i);
        }
        List<ResultEntry> entries = writer.finish();
        ResultEntry entry = entries.get(0);

        org.apache.paimon.fs.Path filePath =
                new org.apache.paimon.fs.Path(archiveDir.resolve(entry.fileName()).toString());
        long fileSize = Files.size(archiveDir.resolve(entry.fileName()));
        GlobalIndexIOMeta ioMeta = new GlobalIndexIOMeta(filePath, fileSize, entry.meta());
        ESIndexGlobalIndexReader reader =
                new ESIndexGlobalIndexReader(
                        new LocalFileReader(), List.of(ioMeta), fields, options);

        FieldRef kRef = new FieldRef(0, "k", DataTypes.STRING());

        Optional<GlobalIndexResult> isNull = reader.visitIsNull(kRef).join();
        assertTrue(isNull.isPresent(), "IS NULL is index-evaluable (not a raw-scan fallback)");
        RoaringNavigableMap64 nullHits = isNull.get().results();
        assertEquals(2, nullHits.getIntCardinality(), "two null rows");
        assertTrue(contains(nullHits, 3L) && contains(nullHits, 7L), "rows 3 and 7 are null");

        Optional<GlobalIndexResult> isNotNull = reader.visitIsNotNull(kRef).join();
        assertTrue(isNotNull.isPresent(), "IS NOT NULL is index-evaluable");
        RoaringNavigableMap64 nn = isNotNull.get().results();
        assertEquals(8, nn.getIntCardinality(), "eight non-null rows");
        assertTrue(!contains(nn, 3L) && !contains(nn, 7L), "null rows excluded from IS NOT NULL");

        Optional<GlobalIndexResult> notRow1 = reader.visitNotEqual(kRef, "row1").join();
        assertTrue(notRow1.isPresent(), "<> on keyword is index-evaluable");
        RoaringNavigableMap64 notRow1Hits = notRow1.get().results();
        assertEquals(7, notRow1Hits.getIntCardinality(), "non-null rows except row1");
        assertTrue(
                !contains(notRow1Hits, 1L)
                        && !contains(notRow1Hits, 3L)
                        && !contains(notRow1Hits, 7L),
                "<> excludes the matching row and null rows");

        Optional<GlobalIndexResult> notRows1And2 =
                reader.visitNotIn(kRef, Arrays.asList("row1", "row2")).join();
        assertTrue(notRows1And2.isPresent(), "NOT IN on keyword is index-evaluable");
        assertEquals(
                6,
                notRows1And2.get().results().getIntCardinality(),
                "non-null rows except row1/row2");

        try {
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void temporalPredicatesUseLongScalarPath(@TempDir java.nio.file.Path tmp) throws IOException {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "d", DataTypes.DATE()),
                        new DataField(1, "ts", DataTypes.TIMESTAMP(3)));
        ESIndexOptions options = new ESIndexOptions(fields, Options.fromMap(new HashMap<>()));

        java.nio.file.Path archiveDir = tmp.resolve("archive-temporal");
        Files.createDirectories(archiveDir);
        LocalDirWriter fileWriter = new LocalDirWriter(archiveDir);
        ESIndexGlobalIndexWriter writer = new ESIndexGlobalIndexWriter(fileWriter, fields, options);
        writer.write(0, GenericRow.of(1, Timestamp.fromEpochMillis(1000L)));
        writer.write(1, GenericRow.of(2, Timestamp.fromEpochMillis(2000L)));
        writer.write(2, GenericRow.of(3, Timestamp.fromEpochMillis(3000L)));
        ResultEntry entry = writer.finish().get(0);

        org.apache.paimon.fs.Path filePath =
                new org.apache.paimon.fs.Path(archiveDir.resolve(entry.fileName()).toString());
        GlobalIndexIOMeta ioMeta =
                new GlobalIndexIOMeta(
                        filePath, Files.size(archiveDir.resolve(entry.fileName())), entry.meta());
        ESIndexGlobalIndexReader reader =
                new ESIndexGlobalIndexReader(
                        new LocalFileReader(), List.of(ioMeta), fields, options);

        FieldRef dateRef = new FieldRef(0, "d", DataTypes.DATE());
        Optional<GlobalIndexResult> dateGe2 = reader.visitGreaterOrEqual(dateRef, 2).join();
        assertTrue(dateGe2.isPresent(), "DATE >= literal is index-evaluable");
        assertEquals(2, dateGe2.get().results().getIntCardinality(), "two rows with d>=2");
        assertTrue(contains(dateGe2.get().results(), 1L) && contains(dateGe2.get().results(), 2L));

        FieldRef tsRef = new FieldRef(1, "ts", DataTypes.TIMESTAMP(3));
        Optional<GlobalIndexResult> tsLt3s =
                reader.visitLessThan(tsRef, Timestamp.fromEpochMillis(3000L)).join();
        assertTrue(tsLt3s.isPresent(), "TIMESTAMP < literal is index-evaluable");
        assertEquals(2, tsLt3s.get().results().getIntCardinality(), "two rows before 3000ms");
        assertTrue(contains(tsLt3s.get().results(), 0L) && contains(tsLt3s.get().results(), 1L));

        Optional<GlobalIndexResult> tsNe2s =
                reader.visitNotEqual(tsRef, Timestamp.fromEpochMillis(2000L)).join();
        assertTrue(tsNe2s.isPresent(), "TIMESTAMP <> literal is index-evaluable");
        assertEquals(2, tsNe2s.get().results().getIntCardinality(), "two rows except 2000ms");
        assertTrue(contains(tsNe2s.get().results(), 0L) && contains(tsNe2s.get().results(), 2L));

        try {
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Verifies the DiskBBQ vector codec write path through {@link ESIndexGlobalIndexWriter}.
     *
     * <p>Distinct from {@link #writeArchiveThenReadAndSearch} (which uses {@code algorithm=hnsw}),
     * this test sets {@code algorithm=diskbbq} so the writer should pick {@code
     * ES920DiskBBQVectorsFormat} via {@code PaimonLucene912Codec.getKnnVectorsFormatForField},
     * causing {@code IVFVectorsWriter#flush} (in practice {@code mergeOneFieldIVF} via {@code
     * IndexWriter.forceMerge(1)}) to emit the IVF triplet ({@code .cenivf} / {@code .clivf} /
     * {@code .mivf}) into the archive instead of the HNSW triplet ({@code .vec} / {@code .vex} /
     * {@code .vem}).
     *
     * <p>Assertions cover the writer path end-to-end (archive contents, scalar/keyword/numeric
     * filters round-tripping through the DiskBBQ-built segment). Vector-search recall through
     * {@code KnnFloatVectorQuery} → {@code DiskBBQVectorsReader.search} is exercised but the recall
     * assertion is lenient — see TODO inline.
     */
    @Test
    void writeDiskBBQArchiveThenReadAndSearch(@TempDir java.nio.file.Path tmp) throws IOException {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "embedding", DataTypes.ARRAY(DataTypes.FLOAT())),
                        new DataField(1, "category", DataTypes.STRING()),
                        new DataField(2, "price", DataTypes.INT()));

        Map<String, String> opt = new HashMap<>();
        opt.put("global-index.es-index.fields.embedding.algorithm", "diskbbq");
        opt.put("global-index.es-index.fields.embedding.dimension", "32");
        opt.put("global-index.es-index.fields.embedding.metric", "l2");
        // 64 is the MIN_VECTORS_PER_CLUSTER allowed by ES920DiskBBQVectorsFormat — picking the
        // minimum lets a small fixture still produce multiple IVF clusters.
        opt.put("global-index.es-index.fields.embedding.vectors_per_cluster", "64");
        ESIndexOptions options = new ESIndexOptions(fields, Options.fromMap(opt));

        java.nio.file.Path archiveDir = tmp.resolve("archive-diskbbq");
        Files.createDirectories(archiveDir);

        // 4 well-separated cluster centers in 32 dims — matches the standalone
        // DiskBBQVectorIndexTests dim and keeps clustering deterministic.
        int dim = 32;
        int clusters = 4;
        int perCluster = 250; // 1000 rows total — well above vectors_per_cluster=64
        float[][] centers = new float[clusters][dim];
        for (int c = 0; c < clusters; c++) {
            centers[c][c * 8] = 100f; // distinct spike per cluster
        }

        LocalDirWriter fileWriter = new LocalDirWriter(archiveDir);
        ESIndexGlobalIndexWriter writer = new ESIndexGlobalIndexWriter(fileWriter, fields, options);
        for (int c = 0; c < clusters; c++) {
            for (int j = 0; j < perCluster; j++) {
                long rowId = (long) c * perCluster + j;
                float[] v = new float[dim];
                for (int d = 0; d < dim; d++) {
                    v[d] = centers[c][d] + ((j % 7) - 3) * 0.001f; // tiny deterministic jitter
                }
                GenericRow row =
                        GenericRow.of(
                                new GenericArray(v), BinaryString.fromString("c" + c), (int) rowId);
                writer.write(rowId, row);
            }
        }
        List<ResultEntry> entries = writer.finish();
        assertEquals(1, entries.size(), "one diskbbq archive produced");
        ResultEntry entry = entries.get(0);
        assertEquals((long) clusters * perCluster, entry.rowCount(), "row count");
        assertNotNull(entry.meta(), "offset-table meta present");

        // --- writer-side check: the archive must contain the DiskBBQ IVF triplet, not HNSW
        // vector files. Parses the offset-table meta (same wire format as
        // ESIndexGlobalIndexReader#parseFileOffsets).
        Set<String> exts = new HashSet<>();
        DataInputStream metaIn = new DataInputStream(new ByteArrayInputStream(entry.meta()));
        int fileCount = metaIn.readInt();
        for (int i = 0; i < fileCount; i++) {
            int nameLen = metaIn.readInt();
            byte[] nameBytes = new byte[nameLen];
            metaIn.readFully(nameBytes);
            metaIn.readLong(); // offset
            metaIn.readLong(); // length
            String name = new String(nameBytes, StandardCharsets.UTF_8);
            int dot = name.lastIndexOf('.');
            if (dot >= 0) {
                exts.add(name.substring(dot + 1));
            }
        }
        assertTrue(
                exts.contains("cenivf") && exts.contains("clivf") && exts.contains("mivf"),
                "archive must contain DiskBBQ IVF triplet (.cenivf/.clivf/.mivf), got: " + exts);
        // .vex (HNSW graph) and .vem (HNSW meta) are HNSW-specific; .vec / .vemf belong to the
        // underlying Lucene99FlatVectorsFormat that DiskBBQ also uses as raw-vector backing
        // storage, so they may legitimately appear with DiskBBQ too.
        assertTrue(
                exts.stream().noneMatch(e -> e.equals("vex") || e.equals("vem")),
                "archive must not contain HNSW-specific files (.vex/.vem), got: " + exts);

        // --- reconstruct reader over the archive ---
        org.apache.paimon.fs.Path filePath =
                new org.apache.paimon.fs.Path(archiveDir.resolve(entry.fileName()).toString());
        long fileSize = Files.size(archiveDir.resolve(entry.fileName()));
        GlobalIndexIOMeta ioMeta = new GlobalIndexIOMeta(filePath, fileSize, entry.meta());

        ESIndexGlobalIndexReader reader =
                new ESIndexGlobalIndexReader(
                        new LocalFileReader(), List.of(ioMeta), fields, options);

        // --- scalar filters round-trip through a DiskBBQ-built segment ---
        FieldRef categoryRef = new FieldRef(1, "category", DataTypes.STRING());
        Optional<GlobalIndexResult> kw = reader.visitEqual(categoryRef, "c1").join();
        assertTrue(kw.isPresent(), "keyword filter on diskbbq index returns a result");
        assertEquals(perCluster, kw.get().results().getIntCardinality(), "perCluster rows in c1");

        // --- numeric filter: price in [perCluster, 2*perCluster) → exactly cluster 1's rows ---
        FieldRef priceRef = new FieldRef(2, "price", DataTypes.INT());
        Optional<GlobalIndexResult> sf = reader.visitGreaterOrEqual(priceRef, perCluster).join();
        Optional<GlobalIndexResult> sfMax = reader.visitLessThan(priceRef, perCluster * 2).join();
        assertTrue(sf.isPresent() && sfMax.isPresent(), "numeric filters return results");
        RoaringNavigableMap64 range =
                RoaringNavigableMap64.and(sf.get().results(), sfMax.get().results());
        assertEquals(perCluster, range.getIntCardinality(), "perCluster rows in [pc, 2*pc)");

        // --- vector search via DiskBBQ: top-5 from cluster-0 center must all be cluster 0. ---
        Optional<ScoredGlobalIndexResult> vr =
                reader.visitVectorSearch(new VectorSearch(centers[0], 5, "embedding")).join();
        assertTrue(vr.isPresent(), "diskbbq vector search returns a result");
        RoaringNavigableMap64 vrows = vr.get().results();
        assertEquals(5, vrows.getIntCardinality(), "k=5 results");
        for (long rid : vrows) {
            assertTrue(
                    rid < perCluster,
                    "row " + rid + " must be from cluster 0 (rowId < " + perCluster + ")");
        }

        try {
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Bypass-archive regression test for the DiskBBQ read path. Writes 1000 random normalised
     * vectors directly to an {@code FSDirectory} via raw Lucene + {@code PaimonLucene912Codec}
     * ({@code algorithm=diskbbq}) — skipping paimon-eslib's archive layer entirely — and reads them
     * back through {@code KnnFloatVectorQuery}.
     *
     * <p>Locks in two fixes in eslib-core {@code lucene9/DiskBBQVectorsReader}:
     *
     * <ul>
     *   <li>{@code search()} now calls {@code knnCollector.incVisitedCount(scoredDocs)} —
     *       previously {@code TopDocs.totalHits} stayed at 0 even when candidates were collected,
     *       making {@code KnnFloatVectorQuery} treat the leaf result as empty.
     *   <li>{@code visitCluster()} now gates {@code collector.collect(...)} on a non-NaN score (and
     *       {@code minCompetitiveSimilarity()}) — previously NaN scores poisoned the collector's
     *       priority queue.
     * </ul>
     */
    @Test
    void diskBBQVectorSearchOnFSDirectory(@TempDir java.nio.file.Path tmp) throws IOException {
        int dim = 32;
        int numVectors = 1000;

        // Match the standalone DiskBBQ test: random normalized vectors + DOT_PRODUCT.
        // (Sparse vectors with EUCLIDEAN produce NaN quantization scores — separate bug.)
        java.util.Random rng = new java.util.Random(42);
        float[][] vectors = new float[numVectors][];
        for (int i = 0; i < numVectors; i++) {
            float[] v = new float[dim];
            float norm = 0;
            for (int d = 0; d < dim; d++) {
                v[d] = rng.nextFloat() - 0.5f;
                norm += v[d] * v[d];
            }
            norm = (float) Math.sqrt(norm);
            for (int d = 0; d < dim; d++) {
                v[d] /= norm;
            }
            vectors[i] = v;
        }
        float[] query = vectors[0]; // query is the first vector — its own row must be top-1

        Map<String, org.elasticsearch.eslib.api.model.FieldIndexConfig> fieldConfigs =
                new HashMap<>();
        Map<String, String> params = new HashMap<>();
        params.put("vectors_per_cluster", "64");
        fieldConfigs.put(
                "embedding",
                org.elasticsearch.eslib.api.model.FieldIndexConfig.builder(
                                "embedding",
                                org.elasticsearch.eslib.api.model.FieldIndexConfig.IndexType.VECTOR)
                        .algorithm(org.elasticsearch.eslib.api.model.VectorAlgorithm.DISKBBQ)
                        .dimension(dim)
                        .metric("dot_product")
                        .algorithmParams(params)
                        .build());

        java.nio.file.Path idxDir = tmp.resolve("fs-diskbbq");
        Files.createDirectories(idxDir);

        // --- write via raw Lucene + PaimonLucene9Codec (the codec paimon-eslib uses;
        // also the only codec registered in Lucene SPI, so DirectoryReader.open finds it back). ---
        org.apache.lucene.codecs.Codec codec =
                new org.elasticsearch.eslib.adapter.lucene9.PaimonLucene9Codec(fieldConfigs);
        org.apache.lucene.index.IndexWriterConfig iwc =
                new org.apache.lucene.index.IndexWriterConfig(
                        new org.apache.lucene.analysis.core.WhitespaceAnalyzer());
        iwc.setCodec(codec);
        iwc.setUseCompoundFile(false);
        try (org.apache.lucene.store.FSDirectory dir =
                        org.apache.lucene.store.FSDirectory.open(idxDir);
                org.apache.lucene.index.IndexWriter w =
                        new org.apache.lucene.index.IndexWriter(dir, iwc)) {
            for (int i = 0; i < numVectors; i++) {
                org.apache.lucene.document.Document doc = new org.apache.lucene.document.Document();
                doc.add(
                        new org.apache.lucene.document.KnnFloatVectorField(
                                "embedding",
                                vectors[i],
                                org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT));
                w.addDocument(doc);
            }
            w.forceMerge(1);
            w.commit();
        }

        // --- list produced files for visibility ---
        Set<String> exts = new HashSet<>();
        try (java.util.stream.Stream<java.nio.file.Path> walk = Files.list(idxDir)) {
            walk.forEach(
                    p -> {
                        String n = p.getFileName().toString();
                        int dot = n.lastIndexOf('.');
                        if (dot >= 0) {
                            exts.add(n.substring(dot + 1));
                        }
                    });
        }
        assertTrue(
                exts.contains("cenivf") && exts.contains("clivf") && exts.contains("mivf"),
                "FSDirectory must hold DiskBBQ IVF triplet, got: " + exts);

        // --- read back via raw Lucene KnnFloatVectorQuery ---
        try (org.apache.lucene.store.FSDirectory dir =
                        org.apache.lucene.store.FSDirectory.open(idxDir);
                org.apache.lucene.index.DirectoryReader r =
                        org.apache.lucene.index.DirectoryReader.open(dir)) {
            assertEquals(numVectors, r.numDocs(), "all docs flushed");
            assertEquals(1, r.leaves().size(), "single segment after forceMerge(1)");

            // Sanity: PerFieldKnnVectorsFormat correctly recorded a DiskBBQ vectors format for this
            // field (version-agnostic: ES920 / ES940 / ...). Reads it back from FieldInfo attrs.
            org.apache.lucene.index.LeafReader leaf = r.leaves().get(0).reader();
            org.apache.lucene.index.FieldInfo fi = leaf.getFieldInfos().fieldInfo("embedding");
            assertTrue(
                    fi.attributes()
                            .get("PerFieldKnnVectorsFormat.format")
                            .contains("DiskBBQVectorsFormat"),
                    "DiskBBQ format must be dispatched at read time");

            // Direct leaf-level KNN — bypasses IndexSearcher / KnnFloatVectorQuery rewrite.
            // Lucene 9.12 returns via the collector (void method).
            org.apache.lucene.search.TopKnnCollector leafCollector =
                    new org.apache.lucene.search.TopKnnCollector(5, Integer.MAX_VALUE);
            leaf.searchNearestVectors("embedding", query, leafCollector, null);
            org.apache.lucene.search.TopDocs leafTop = leafCollector.topDocs();

            // Document the bug: DiskBBQVectorsReader.search DOES collect documents …
            assertTrue(
                    leafTop.scoreDocs.length > 0,
                    "DiskBBQVectorsReader.search should collect candidates");
            for (org.apache.lucene.search.ScoreDoc sd : leafTop.scoreDocs) {
                assertTrue(
                        Float.isFinite(sd.score),
                        "score must be finite (NaN guard / simdvec stats fix), got: " + sd.score);
            }
            assertTrue(
                    leafTop.totalHits.value > 0,
                    "TopDocs.totalHits.value must be > 0 (incVisitedCount fix)");

            // The wrapping KnnFloatVectorQuery path must now return the requested k hits, with
            // the query vector itself (doc 0) among them.
            // KnnFloatVectorQuery on top of the leaf must now produce k=5 finite-scored hits
            // (before the fixes it produced MatchNoDocsQuery because the FloatVectorValues stub
            // reported size==0, which short-circuited KnnFloatVectorQuery.approximateSearch).
            org.apache.lucene.search.IndexSearcher s =
                    new org.apache.lucene.search.IndexSearcher(r);
            org.apache.lucene.search.KnnFloatVectorQuery q =
                    new org.apache.lucene.search.KnnFloatVectorQuery("embedding", query, 5);
            org.apache.lucene.search.TopDocs td = s.search(q, 5);
            assertEquals(5, td.scoreDocs.length, "KnnFloatVectorQuery returns k=5 hits");
            for (org.apache.lucene.search.ScoreDoc sd : td.scoreDocs) {
                assertTrue(Float.isFinite(sd.score), "score must be finite, got: " + sd.score);
                assertTrue(
                        sd.score > 0, "score must be positive for DOT_PRODUCT, got: " + sd.score);
            }
            // Recall (whether the query vector itself appears in top-5) is intentionally NOT
            // asserted: DiskBBQ is an approximate IVF method whose recall depends on nprobe and
            // clustering quality; with vpc=64 + 1000 docs we visit only 4/~16 clusters by default.
            // The bug-fix verification is "finite scores + non-zero hits" — see Javadoc.
        }
    }

    private static String matchQuery(String terms) {
        return "{\"match\":{\"query\":\"" + terms + "\"}}";
    }

    private static String matchQuery(String terms, String operator) {
        return "{\"match\":{\"query\":\"" + terms + "\",\"operator\":\"" + operator + "\"}}";
    }

    private static String fuzzyMatchQuery(String terms, int fuzziness) {
        return "{\"match\":{\"query\":\"" + terms + "\",\"fuzziness\":" + fuzziness + "}}";
    }

    private static String phraseQuery(String terms) {
        return "{\"match_phrase\":{\"query\":\"" + terms + "\"}}";
    }

    private static boolean contains(RoaringNavigableMap64 bitmap, long id) {
        for (long v : bitmap) {
            if (v == id) {
                return true;
            }
        }
        return false;
    }
}
