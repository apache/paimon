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
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
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

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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

    @Test
    void writeArchiveThenReadAndSearch(@TempDir java.nio.file.Path tmp) throws IOException {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "embedding", DataTypes.ARRAY(DataTypes.FLOAT())),
                        new DataField(1, "title", DataTypes.STRING()),
                        new DataField(2, "category", DataTypes.STRING()),
                        new DataField(3, "price", DataTypes.INT()));

        Map<String, String> opt = new HashMap<>();
        opt.put("fields.embedding.algorithm", "hnsw");
        opt.put("fields.embedding.dimension", "4");
        opt.put("fields.embedding.metric", "l2");
        opt.put("fields.title.analyzer", "standard");
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
            writer.write(row);
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
                reader.visitVectorSearch(
                        new VectorSearch(new float[] {0, 0, 0, 0}, 5, "embedding"));
        assertTrue(vr.isPresent(), "vector search returns a result");
        RoaringNavigableMap64 vrows = vr.get().results();
        assertEquals(5, vrows.getIntCardinality(), "k=5 results");
        assertTrue(contains(vrows, 0L), "row 0 (origin) recalled by HNSW");

        // --- full-text search: 10 rows contain "even" ---
        Optional<ScoredGlobalIndexResult> ft =
                reader.visitFullTextSearch(new FullTextSearch("even", 50, "title"));
        assertTrue(ft.isPresent(), "full-text search returns a result");
        assertEquals(10, ft.get().results().getIntCardinality(), "10 even docs");

        // --- scalar filter: price >= 100 means rows 10..19 (price = i*10) ---
        FieldRef priceRef = new FieldRef(3, "price", DataTypes.INT());
        Optional<GlobalIndexResult> sf = reader.visitGreaterOrEqual(priceRef, 100);
        assertTrue(sf.isPresent(), "scalar filter returns a result");
        assertEquals(10, sf.get().results().getIntCardinality(), "10 rows with price >= 100");

        // --- scalar filter: price == 50 → row 5 only ---
        Optional<GlobalIndexResult> eq = reader.visitEqual(priceRef, 50);
        assertTrue(eq.isPresent(), "scalar eq filter returns a result");
        assertEquals(1, eq.get().results().getIntCardinality(), "1 row with price == 50");
        assertTrue(contains(eq.get().results(), 5L), "row 5 has price=50");

        // --- keyword filter: category == "even" → 10 rows ---
        FieldRef categoryRef = new FieldRef(2, "category", DataTypes.STRING());
        Optional<GlobalIndexResult> kw = reader.visitEqual(categoryRef, "even");
        assertTrue(kw.isPresent(), "keyword eq filter returns a result");
        assertEquals(10, kw.get().results().getIntCardinality(), "10 even category rows");

        // --- keyword prefix: category startsWith "ev" → 10 rows with "even" ---
        Optional<GlobalIndexResult> sw = reader.visitStartsWith(categoryRef, "ev");
        assertTrue(sw.isPresent(), "startsWith filter returns a result");
        assertEquals(10, sw.get().results().getIntCardinality(), "10 rows start with 'ev'");

        // --- keyword contains: category contains "dd" → 10 rows with "odd" ---
        Optional<GlobalIndexResult> ct = reader.visitContains(categoryRef, "dd");
        assertTrue(ct.isPresent(), "contains filter returns a result");
        assertEquals(10, ct.get().results().getIntCardinality(), "10 rows contain 'dd'");

        try {
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
