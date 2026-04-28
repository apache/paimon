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

package org.apache.paimon.globalindex.btree;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.GlobalIndexFileReadWrite;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestEntrySerializer;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;

import static org.apache.paimon.io.DataFileTestUtils.newFile;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests verifying that {@link BinaryFileMetaIndexReader.EntryIterator} exposes keys correctly
 * and that the {@link LinkedHashMap}-based deduplication in {@link BTreeWithFileMetaReader}
 * eliminates duplicate {@code fileName} entries produced by parallel subtasks writing identical
 * {@code btree_file_meta} SST files.
 */
public class BinaryFileMetaDeduplicationTest {

    @TempDir java.nio.file.Path tempDir;

    /** Write a single {@code btree_file_meta} SST with the given fileName -> entry mappings. */
    private List<ResultEntry> writeSst(
            GlobalIndexFileReadWrite rw,
            ManifestEntrySerializer serializer,
            List<ManifestEntry> entries)
            throws IOException {
        BinaryFileMetaWriter writer = new BinaryFileMetaWriter(rw, serializer);
        for (ManifestEntry entry : entries) {
            writer.write(entry.file().fileName(), entry);
        }
        return writer.finish();
    }

    private GlobalIndexIOMeta toIOMeta(LocalFileIO fileIO, IndexPathFactory factory, String name)
            throws IOException {
        Path p = factory.toPath(name);
        return new GlobalIndexIOMeta(p, fileIO.getFileSize(p), null);
    }

    // -------------------------------------------------------------------------
    // Test 1: EntryIterator correctly exposes both key and value
    // -------------------------------------------------------------------------

    @Test
    public void testEntryIteratorExposesKeyAndValue() throws IOException {
        LocalFileIO fileIO = new LocalFileIO();
        Path dir = new Path(tempDir.toString());
        IndexPathFactory factory = new SimpleIndexPathFactory(dir);
        GlobalIndexFileReadWrite rw = new GlobalIndexFileReadWrite(fileIO, factory);
        ManifestEntrySerializer serializer = new ManifestEntrySerializer();

        ManifestEntry entry1 = makeEntry("data-file-aaa.parquet");
        ManifestEntry entry2 = makeEntry("data-file-bbb.parquet");

        List<ManifestEntry> entries = new ArrayList<>();
        entries.add(entry1);
        entries.add(entry2);
        List<ResultEntry> results = writeSst(rw, serializer, entries);
        assertThat(results).hasSize(1);

        GlobalIndexIOMeta meta = toIOMeta(fileIO, factory, results.get(0).fileName());
        Options options = new Options();
        try (BinaryFileMetaIndexReader reader = new BinaryFileMetaIndexReader(rw, meta, options)) {
            BinaryFileMetaIndexReader.EntryIterator it = reader.iterator();

            // First entry
            assertThat(it.hasNext()).isTrue();
            assertThat(it.nextKey()).isEqualTo("data-file-aaa.parquet");
            it.nextValue(); // advance

            // Second entry
            assertThat(it.hasNext()).isTrue();
            assertThat(it.nextKey()).isEqualTo("data-file-bbb.parquet");
            it.nextValue();

            assertThat(it.hasNext()).isFalse();
        }
    }

    // -------------------------------------------------------------------------
    // Test 2: Identical SSTs from N parallel subtasks are deduplicated by fileName
    // -------------------------------------------------------------------------

    @Test
    public void testDuplicateSstDeduplicationByFileName() throws IOException {
        LocalFileIO fileIO = new LocalFileIO();
        Path dir = new Path(tempDir.toString());
        IndexPathFactory factory = new SimpleIndexPathFactory(dir);
        GlobalIndexFileReadWrite rw = new GlobalIndexFileReadWrite(fileIO, factory);
        ManifestEntrySerializer serializer = new ManifestEntrySerializer();

        ManifestEntry e1 = makeEntry("data-file-001.parquet");
        ManifestEntry e2 = makeEntry("data-file-002.parquet");
        ManifestEntry e3 = makeEntry("data-file-003.parquet");

        List<ManifestEntry> entries = new ArrayList<>();
        entries.add(e1);
        entries.add(e2);
        entries.add(e3);

        // Simulate 3 parallel subtasks each writing an identical btree_file_meta SST
        List<GlobalIndexIOMeta> metas = new ArrayList<>();
        int parallelism = 3;
        for (int i = 0; i < parallelism; i++) {
            List<ResultEntry> results = writeSst(rw, serializer, entries);
            assertThat(results).hasSize(1);
            metas.add(toIOMeta(fileIO, factory, results.get(0).fileName()));
        }
        assertThat(metas).hasSize(parallelism);

        // Simulate what BTreeWithFileMetaReader.readAllBinaryFileMetaEntries() does
        LinkedHashMap<String, byte[]> seen = new LinkedHashMap<>();
        Options options = new Options();
        for (GlobalIndexIOMeta meta : metas) {
            try (BinaryFileMetaIndexReader reader =
                    new BinaryFileMetaIndexReader(rw, meta, options)) {
                BinaryFileMetaIndexReader.EntryIterator it = reader.iterator();
                while (it.hasNext()) {
                    String fileName = it.nextKey();
                    byte[] valueBytes = it.nextValue();
                    seen.putIfAbsent(fileName, valueBytes);
                }
            }
        }

        // Despite 3 SST files each containing the same 3 fileNames, deduplication must yield
        // exactly 3 distinct ManifestEntry bytes — not 3 * 3 = 9.
        assertThat(seen).hasSize(3);
        assertThat(seen.keySet())
                .containsExactly(
                        "data-file-001.parquet", "data-file-002.parquet", "data-file-003.parquet");
    }

    // -------------------------------------------------------------------------
    // Test 3: Partially-overlapping SSTs are correctly merged
    // -------------------------------------------------------------------------

    @Test
    public void testPartiallyOverlappingSstsAreMerged() throws IOException {
        LocalFileIO fileIO = new LocalFileIO();
        Path dir = new Path(tempDir.toString());
        IndexPathFactory factory = new SimpleIndexPathFactory(dir);
        GlobalIndexFileReadWrite rw = new GlobalIndexFileReadWrite(fileIO, factory);
        ManifestEntrySerializer serializer = new ManifestEntrySerializer();

        // SST A: file-001, file-002
        List<ManifestEntry> setA = new ArrayList<>();
        setA.add(makeEntry("data-file-001.parquet"));
        setA.add(makeEntry("data-file-002.parquet"));
        List<ResultEntry> resA = writeSst(rw, serializer, setA);

        // SST B: file-002 (duplicate), file-003 (new)
        List<ManifestEntry> setB = new ArrayList<>();
        setB.add(makeEntry("data-file-002.parquet"));
        setB.add(makeEntry("data-file-003.parquet"));
        List<ResultEntry> resB = writeSst(rw, serializer, setB);

        List<GlobalIndexIOMeta> metas = new ArrayList<>();
        metas.add(toIOMeta(fileIO, factory, resA.get(0).fileName()));
        metas.add(toIOMeta(fileIO, factory, resB.get(0).fileName()));

        LinkedHashMap<String, byte[]> seen = new LinkedHashMap<>();
        Options options = new Options();
        for (GlobalIndexIOMeta meta : metas) {
            try (BinaryFileMetaIndexReader reader =
                    new BinaryFileMetaIndexReader(rw, meta, options)) {
                BinaryFileMetaIndexReader.EntryIterator it = reader.iterator();
                while (it.hasNext()) {
                    String fileName = it.nextKey();
                    byte[] valueBytes = it.nextValue();
                    seen.putIfAbsent(fileName, valueBytes);
                }
            }
        }

        // file-002 appears in both SSTs but must appear exactly once
        assertThat(seen).hasSize(3);
        assertThat(seen.keySet())
                .containsExactlyInAnyOrder(
                        "data-file-001.parquet", "data-file-002.parquet", "data-file-003.parquet");
    }

    // -------------------------------------------------------------------------
    // Helper: create a minimal ManifestEntry with the given data file name
    // -------------------------------------------------------------------------

    private static ManifestEntry makeEntry(String dataFileName) {
        return ManifestEntry.create(
                FileKind.ADD, BinaryRow.EMPTY_ROW, 0, 1, newFile(dataFileName, 0, 0, 99, 0L));
    }

    // -------------------------------------------------------------------------
    // Minimal IndexPathFactory backed by a temp directory
    // -------------------------------------------------------------------------

    private static class SimpleIndexPathFactory implements IndexPathFactory {

        private final Path dir;

        SimpleIndexPathFactory(Path dir) {
            this.dir = dir;
        }

        @Override
        public Path toPath(String fileName) {
            return new Path(dir, fileName);
        }

        @Override
        public Path newPath() {
            return new Path(dir, UUID.randomUUID().toString() + ".index");
        }

        @Override
        public boolean isExternalPath() {
            return false;
        }
    }
}
