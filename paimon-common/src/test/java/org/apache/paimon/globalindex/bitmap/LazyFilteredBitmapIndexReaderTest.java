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

package org.apache.paimon.globalindex.bitmap;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.SeekableInputStreamWrapper;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.Pair;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.shade.guava30.com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LazyFilteredBitmapReader}. */
public class LazyFilteredBitmapIndexReaderTest {

    private final VarCharType dataType = new VarCharType(VarCharType.MAX_LENGTH);
    private final DataField dataField = new DataField(1, "tag", dataType);
    private final FieldRef fieldRef = new FieldRef(1, "tag", dataType);

    private FileIO fileIO;
    private Path basePath;
    private GlobalIndexFileWriter fileWriter;
    private GlobalIndexFileReader fileReader;
    private BitmapGlobalIndexer globalIndexer;

    @TempDir java.nio.file.Path tempPath;

    @BeforeEach
    public void setUp() {
        fileIO = LocalFileIO.create();
        basePath = new Path(tempPath.toUri());
        fileWriter =
                new GlobalIndexFileWriter() {
                    @Override
                    public String newFileName(String prefix) {
                        return prefix + "-" + UUID.randomUUID() + ".index";
                    }

                    @Override
                    public PositionOutputStream newOutputStream(String fileName)
                            throws IOException {
                        return fileIO.newOutputStream(new Path(basePath, fileName), true);
                    }
                };
        fileReader = meta -> fileIO.newInputStream(meta.filePath());
        globalIndexer = new BitmapGlobalIndexer(dataField, new Options());
    }

    @Test
    public void testEqualityFamilyPredicates() throws Exception {
        disableFallbackScan();
        GlobalIndexIOMeta meta =
                writeData(
                        Arrays.asList(
                                Pair.of(str("A"), 0L),
                                Pair.of(str("B"), 1L),
                                Pair.of(null, 2L),
                                Pair.of(str("B"), 3L),
                                Pair.of(str("C"), 4L),
                                Pair.of(str("A"), 5L)));

        try (GlobalIndexReader reader =
                globalIndexer.createReader(
                        fileReader, Collections.singletonList(meta), newDirectExecutorService())) {
            assertRows(reader.visitEqual(fieldRef, str("A")).join(), 0L, 5L);
            assertRows(reader.visitEqual(fieldRef, null).join());
            assertRows(reader.visitEqual(fieldRef, str("missing")).join());
            assertRows(
                    reader.visitIn(fieldRef, Arrays.asList(str("B"), str("C"))).join(), 1L, 3L, 4L);
            assertRows(reader.visitIn(fieldRef, Arrays.asList(str("A"), null)).join(), 0L, 5L);
            assertRows(reader.visitStartsWith(fieldRef, str("A")).join(), 0L, 5L);
            assertRows(reader.visitStartsWith(fieldRef, str("")).join(), 0L, 1L, 3L, 4L, 5L);
            assertRows(reader.visitLike(fieldRef, str("A")).join(), 0L, 5L);
            assertRows(reader.visitLike(fieldRef, str("A%")).join(), 0L, 5L);
            assertRows(reader.visitNotEqual(fieldRef, str("A")).join(), 1L, 3L, 4L);
            assertRows(reader.visitNotEqual(fieldRef, null).join());
            assertRows(
                    reader.visitNotIn(fieldRef, Arrays.asList(str("B"), str("C"))).join(), 0L, 5L);
            assertRows(reader.visitNotIn(fieldRef, Arrays.asList(str("B"), null)).join());
            assertRows(reader.visitIsNull(fieldRef).join(), 2L);
            assertRows(reader.visitIsNotNull(fieldRef).join(), 0L, 1L, 3L, 4L, 5L);

            assertThat(reader.visitLessThan(fieldRef, str("B")).join()).isEmpty();
            assertThat(reader.visitBetween(fieldRef, str("A"), str("C")).join()).isEmpty();
            assertThat(reader.visitEndsWith(fieldRef, str("A")).join()).isEmpty();
            assertThat(reader.visitContains(fieldRef, str("A")).join()).isEmpty();
            assertThat(reader.visitLike(fieldRef, str("%A")).join()).isEmpty();
        }
    }

    @Test
    public void testFallbackScanPredicates() throws Exception {
        GlobalIndexIOMeta meta =
                writeData(
                        Arrays.asList(
                                Pair.of(str("alpha"), 0L),
                                Pair.of(str("beta"), 1L),
                                Pair.of(str("alphabet"), 2L),
                                Pair.of(str("delta"), 3L),
                                Pair.of(null, 4L)));

        try (GlobalIndexReader reader =
                globalIndexer.createReader(
                        fileReader, Collections.singletonList(meta), newDirectExecutorService())) {
            assertRows(reader.visitEndsWith(fieldRef, str("ta")).join(), 1L, 3L);
            assertRows(reader.visitContains(fieldRef, str("ph")).join(), 0L, 2L);
            assertRows(reader.visitLike(fieldRef, str("%ha%")).join(), 0L, 2L);
            assertRows(reader.visitLessThan(fieldRef, str("delta")).join(), 0L, 1L, 2L);
            assertRows(reader.visitLessOrEqual(fieldRef, str("beta")).join(), 0L, 1L, 2L);
            assertRows(reader.visitGreaterThan(fieldRef, str("beta")).join(), 3L);
            assertRows(reader.visitGreaterOrEqual(fieldRef, str("beta")).join(), 1L, 3L);
            assertRows(reader.visitBetween(fieldRef, str("beta"), str("delta")).join(), 1L, 3L);
        }
    }

    @Test
    public void testCompressedDictionaryBlocks() throws Exception {
        Options compressedOptions = new Options();
        compressedOptions.set(BitmapGlobalIndexOptions.BITMAP_INDEX_COMPRESSION, "lz4");
        compressedOptions.set(
                BitmapGlobalIndexOptions.BITMAP_INDEX_DICTIONARY_BLOCK_SIZE,
                org.apache.paimon.options.MemorySize.ofKibiBytes(256));
        globalIndexer = new BitmapGlobalIndexer(dataField, compressedOptions);

        List<Pair<BinaryString, Long>> rows = new ArrayList<>();
        String prefix =
                "very-long-common-prefix-for-bitmap-dictionary-compression-"
                        + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-";
        for (int i = 0; i < 300; i++) {
            rows.add(Pair.of(str(String.format("%s%05d", prefix, i)), (long) i));
        }
        GlobalIndexIOMeta compressed = writeData(rows);

        Options plainOptions = new Options();
        plainOptions.set(
                BitmapGlobalIndexOptions.BITMAP_INDEX_DICTIONARY_BLOCK_SIZE,
                org.apache.paimon.options.MemorySize.ofKibiBytes(256));
        globalIndexer = new BitmapGlobalIndexer(dataField, plainOptions);
        GlobalIndexIOMeta plain = writeData(rows);

        assertThat(compressed.fileSize()).isLessThan(plain.fileSize());

        try (GlobalIndexReader reader =
                globalIndexer.createReader(
                        fileReader,
                        Collections.singletonList(compressed),
                        newDirectExecutorService())) {
            assertRows(reader.visitEqual(fieldRef, str(prefix + "00123")).join(), 123L);
            assertRows(
                    reader.visitStartsWith(fieldRef, str(prefix + "0012")).join(),
                    120L,
                    121L,
                    122L,
                    123L,
                    124L,
                    125L,
                    126L,
                    127L,
                    128L,
                    129L);
        }
    }

    @Test
    public void testFallbackScanDisabledByBudget() throws Exception {
        Options options = new Options();
        options.set(
                BitmapGlobalIndexOptions.BITMAP_INDEX_FALLBACK_SCAN_MAX_SIZE,
                org.apache.paimon.options.MemorySize.ofBytes(1));
        globalIndexer = new BitmapGlobalIndexer(dataField, options);

        GlobalIndexIOMeta meta =
                writeData(Arrays.asList(Pair.of(str("alpha"), 0L), Pair.of(str("beta"), 1L)));

        try (GlobalIndexReader reader =
                globalIndexer.createReader(
                        fileReader, Collections.singletonList(meta), newDirectExecutorService())) {
            assertThat(reader.visitEndsWith(fieldRef, str("ta")).join()).isEmpty();
            assertThat(reader.visitContains(fieldRef, str("ph")).join()).isEmpty();
            assertThat(reader.visitLike(fieldRef, str("%ha%")).join()).isEmpty();
            assertThat(reader.visitLessThan(fieldRef, str("beta")).join()).isEmpty();
            assertRows(reader.visitEqual(fieldRef, str("beta")).join(), 1L);
        }
    }

    @Test
    public void testMultiFileComplementsUsePerFileOwnedRows() throws Exception {
        GlobalIndexIOMeta first =
                writeData(
                        Arrays.asList(
                                Pair.of(str("A"), 0L), Pair.of(str("B"), 1L), Pair.of(null, 2L)));
        GlobalIndexIOMeta second =
                writeData(
                        Arrays.asList(
                                Pair.of(str("B"), 3L),
                                Pair.of(str("C"), 4L),
                                Pair.of(str("A"), 5L)));

        try (GlobalIndexReader reader =
                globalIndexer.createReader(
                        fileReader, Arrays.asList(first, second), newDirectExecutorService())) {
            assertRows(reader.visitEqual(fieldRef, str("B")).join(), 1L, 3L);
            assertRows(reader.visitNotEqual(fieldRef, str("A")).join(), 1L, 3L, 4L);
            assertRows(
                    reader.visitNotIn(fieldRef, Collections.singletonList(str("B"))).join(),
                    0L,
                    4L,
                    5L);
            assertRows(reader.visitIsNotNull(fieldRef).join(), 0L, 1L, 3L, 4L, 5L);
        }
    }

    @Test
    public void testManifestMetaPrunesFilesBeforeLookup() throws Exception {
        GlobalIndexIOMeta first =
                writeData(Arrays.asList(Pair.of(str("A"), 0L), Pair.of(str("B"), 1L)));
        GlobalIndexIOMeta second =
                writeData(Arrays.asList(Pair.of(str("Y"), 2L), Pair.of(str("Z"), 3L)));

        CountingGlobalIndexFileReader countingFileReader = new CountingGlobalIndexFileReader();
        try (GlobalIndexReader reader =
                globalIndexer.createReader(
                        countingFileReader,
                        Arrays.asList(first, second),
                        newDirectExecutorService())) {
            assertRows(reader.visitEqual(fieldRef, str("Z")).join(), 3L);

            assertThat(countingFileReader.openCount())
                    .as("manifest min/max should skip the first index file")
                    .isEqualTo(1);

            assertRows(reader.visitStartsWith(fieldRef, str("Z")).join(), 3L);
            assertThat(countingFileReader.openCount())
                    .as("manifest prefix range should also skip the first index file")
                    .isEqualTo(1);

            assertRows(reader.visitNotEqual(fieldRef, null).join());
            assertThat(countingFileReader.openCount())
                    .as("null complement should not open extra index files")
                    .isEqualTo(1);
        }
    }

    @Test
    public void testFallbackBudgetUsesSelectedFiles() throws Exception {
        GlobalIndexIOMeta first =
                writeData(Arrays.asList(Pair.of(str("A"), 0L), Pair.of(str("B"), 1L)));
        GlobalIndexIOMeta second =
                writeData(Arrays.asList(Pair.of(str("Y"), 2L), Pair.of(str("Z"), 3L)));

        Options options = new Options();
        options.set(
                BitmapGlobalIndexOptions.BITMAP_INDEX_FALLBACK_SCAN_MAX_SIZE,
                org.apache.paimon.options.MemorySize.ofBytes(second.fileSize()));
        globalIndexer = new BitmapGlobalIndexer(dataField, options);

        try (GlobalIndexReader reader =
                globalIndexer.createReader(
                        fileReader, Arrays.asList(first, second), newDirectExecutorService())) {
            assertRows(reader.visitGreaterOrEqual(fieldRef, str("Y")).join(), 2L, 3L);
            assertThat(reader.visitContains(fieldRef, str("Z")).join()).isEmpty();
        }
    }

    @Test
    public void testStartsWithAcrossDictionaryBlockBoundary() throws Exception {
        Options options = new Options();
        options.set(
                BitmapGlobalIndexOptions.BITMAP_INDEX_DICTIONARY_BLOCK_SIZE,
                org.apache.paimon.options.MemorySize.ofBytes(32));
        globalIndexer = new BitmapGlobalIndexer(dataField, options);

        GlobalIndexIOMeta meta =
                writeData(
                        Arrays.asList(
                                Pair.of(str("tag"), 1L),
                                Pair.of(str("tag-000"), 2L),
                                Pair.of(str("tag-001"), 3L),
                                Pair.of(str("tag."), 4L),
                                Pair.of(str("zzz"), 5L)));

        try (GlobalIndexReader reader =
                globalIndexer.createReader(
                        fileReader, Collections.singletonList(meta), newDirectExecutorService())) {
            assertRows(reader.visitStartsWith(fieldRef, str("tag-")).join(), 2L, 3L);
        }
    }

    @Test
    public void testStartsWithReadsOnlyMatchingDictionaryBlocks() throws Exception {
        Options options = new Options();
        options.set(
                BitmapGlobalIndexOptions.BITMAP_INDEX_DICTIONARY_BLOCK_SIZE,
                org.apache.paimon.options.MemorySize.ofBytes(32));
        globalIndexer = new BitmapGlobalIndexer(dataField, options);

        List<Pair<BinaryString, Long>> rows = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            rows.add(Pair.of(str(String.format("other-%03d", i)), (long) i));
        }
        for (int i = 0; i < 3; i++) {
            rows.add(Pair.of(str(String.format("tag-match-%03d", i)), 100L + i));
        }
        for (int i = 0; i < 50; i++) {
            rows.add(Pair.of(str(String.format("zzz-%03d", i)), 200L + i));
        }
        GlobalIndexIOMeta meta = writeData(rows);

        CountingGlobalIndexFileReader countingFileReader = new CountingGlobalIndexFileReader();
        try (GlobalIndexReader reader =
                globalIndexer.createReader(
                        countingFileReader,
                        Collections.singletonList(meta),
                        newDirectExecutorService())) {
            assertRows(reader.visitStartsWith(fieldRef, str("tag-match")).join(), 100L, 101L, 102L);

            assertThat(countingFileReader.seekCount())
                    .as("footer, index, matching dictionary blocks and bitmaps")
                    .isLessThanOrEqualTo(9);
        }
    }

    @Test
    public void testEqualDoesNotReadAllDictionaryBlocks() throws Exception {
        Options options = new Options();
        options.set(
                BitmapGlobalIndexOptions.BITMAP_INDEX_DICTIONARY_BLOCK_SIZE,
                org.apache.paimon.options.MemorySize.ofBytes(32));
        globalIndexer = new BitmapGlobalIndexer(dataField, options);

        List<Pair<BinaryString, Long>> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            rows.add(Pair.of(str(String.format("tag-%03d", i)), (long) i));
        }
        GlobalIndexIOMeta meta = writeData(rows);

        CountingGlobalIndexFileReader countingFileReader = new CountingGlobalIndexFileReader();
        try (GlobalIndexReader reader =
                globalIndexer.createReader(
                        countingFileReader,
                        Collections.singletonList(meta),
                        newDirectExecutorService())) {
            assertRows(reader.visitEqual(fieldRef, str("tag-050")).join(), 50L);

            assertThat(countingFileReader.seekCount())
                    .as("footer, index, one dictionary block and one bitmap")
                    .isLessThanOrEqualTo(4);
        }
    }

    @Test
    public void testNullChecksDoNotReadDictionary() throws Exception {
        GlobalIndexIOMeta meta =
                writeData(
                        Arrays.asList(
                                Pair.of(str("A"), 0L), Pair.of(str("B"), 1L), Pair.of(null, 2L)));

        CountingGlobalIndexFileReader countingFileReader = new CountingGlobalIndexFileReader();
        try (GlobalIndexReader reader =
                globalIndexer.createReader(
                        countingFileReader,
                        Collections.singletonList(meta),
                        newDirectExecutorService())) {
            assertRows(reader.visitIsNull(fieldRef).join(), 2L);

            assertThat(countingFileReader.seekCount())
                    .as("footer and null bitmap")
                    .isLessThanOrEqualTo(2);
        }
    }

    private GlobalIndexIOMeta writeData(List<Pair<BinaryString, Long>> data) throws IOException {
        GlobalIndexSingleColumnWriter writer = globalIndexer.createWriter(fileWriter);
        for (Pair<BinaryString, Long> pair : data) {
            writer.write(pair.getKey(), pair.getValue());
        }

        List<ResultEntry> results = writer.finish();
        assertThat(results).hasSize(1);
        ResultEntry resultEntry = results.get(0);
        Path filePath = new Path(basePath, resultEntry.fileName());
        return new GlobalIndexIOMeta(filePath, fileIO.getFileSize(filePath), resultEntry.meta());
    }

    private static BinaryString str(String value) {
        return BinaryString.fromString(value);
    }

    private void disableFallbackScan() {
        Options options = new Options();
        options.set(
                BitmapGlobalIndexOptions.BITMAP_INDEX_FALLBACK_SCAN_MAX_SIZE,
                org.apache.paimon.options.MemorySize.ofBytes(0));
        globalIndexer = new BitmapGlobalIndexer(dataField, options);
    }

    private static void assertRows(Optional<GlobalIndexResult> result, Long... expected) {
        assertThat(result).isPresent();

        Iterator<Long> iterator = result.get().results().iterator();
        List<Long> actual = new ArrayList<>();
        while (iterator.hasNext()) {
            actual.add(iterator.next());
        }
        assertThat(actual).containsExactlyInAnyOrder(expected);
    }

    private class CountingGlobalIndexFileReader implements GlobalIndexFileReader {

        private final AtomicInteger seekCount = new AtomicInteger();
        private final AtomicInteger openCount = new AtomicInteger();

        @Override
        public SeekableInputStream getInputStream(GlobalIndexIOMeta meta) throws IOException {
            openCount.incrementAndGet();
            return new CountingSeekableInputStream(fileReader.getInputStream(meta), seekCount);
        }

        int seekCount() {
            return seekCount.get();
        }

        int openCount() {
            return openCount.get();
        }
    }

    private static class CountingSeekableInputStream extends SeekableInputStreamWrapper {

        private final AtomicInteger seekCount;

        private CountingSeekableInputStream(SeekableInputStream wrapped, AtomicInteger seekCount) {
            super(wrapped);
            this.seekCount = seekCount;
        }

        @Override
        public void seek(long desired) throws IOException {
            if (desired != getPos()) {
                seekCount.incrementAndGet();
            }
            super.seek(desired);
        }
    }
}
