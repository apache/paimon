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

package org.apache.paimon.globalindex.fmindex;

import org.apache.paimon.compression.BlockCompressionType;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.SeekableInputStreamWrapper;
import org.apache.paimon.fs.VectoredReadable;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.VarCharType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.paimon.shade.guava30.com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Behavioral contract tests for the exact partitioned FM contains index. */
public class FMGlobalIndexTest {

    private final VarCharType dataType = new VarCharType(VarCharType.MAX_LENGTH);
    private final DataField dataField = new DataField(1, "text", dataType);
    private final FieldRef fieldRef = new FieldRef(1, "text", dataType);

    private FileIO fileIO;
    private Path basePath;
    private GlobalIndexFileWriter fileWriter;
    private GlobalIndexFileReader fileReader;
    private FMGlobalIndexer indexer;

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

        Options options = new Options();
        options.set(FMGlobalIndexOptions.PARTITION_ROW_COUNT, 3);
        options.set(FMGlobalIndexOptions.SA_SAMPLE_RATE, 1);
        options.set(FMGlobalIndexOptions.COMPRESSION, "lz4");
        options.set(FMGlobalIndexOptions.LOCATE_COST_RATIO, 1d);
        indexer = new FMGlobalIndexer(dataField, options);
    }

    @Test
    public void testExactContainsForShortUtf8AndRepeatedOccurrences() throws Exception {
        List<GlobalIndexIOMeta> files =
                writeData(
                        Arrays.asList(
                                str("abcdef"),
                                str("aaaaaa"),
                                null,
                                str(""),
                                str("你好世界"),
                                str("tail-abcdef-tail"),
                                str("ab")),
                        0);

        try (GlobalIndexReader reader = createReader(files, 7)) {
            assertRows(reader.visitContains(fieldRef, str("a")).join(), 0L, 1L, 5L, 6L);
            assertRows(reader.visitContains(fieldRef, str("ab")).join(), 0L, 5L, 6L);
            assertRows(reader.visitContains(fieldRef, str("abcdef")).join(), 0L, 5L);
            assertRows(reader.visitContains(fieldRef, str("好")).join(), 4L);
            assertRows(reader.visitContains(fieldRef, str("missing")).join());
            assertRows(reader.visitContains(fieldRef, str("")).join(), 0L, 1L, 3L, 4L, 5L, 6L);
            assertRows(reader.visitContains(fieldRef, null).join());
        }
    }

    @Test
    public void testSeparatorPreventsCrossRowMatchesAndAllBytesAreSearchable() throws Exception {
        byte[] first = {(byte) 0x00, (byte) 0xFF, 'a'};
        byte[] second = {'b', (byte) 0x00, (byte) 0xFF};
        List<GlobalIndexIOMeta> files =
                writeData(
                        Arrays.asList(
                                BinaryString.fromBytes(first), BinaryString.fromBytes(second)),
                        0);

        try (GlobalIndexReader reader = createReader(files, 2)) {
            assertRows(
                    reader.visitContains(
                                    fieldRef,
                                    BinaryString.fromBytes(new byte[] {(byte) 0x00, (byte) 0xFF}))
                            .join(),
                    0L,
                    1L);
            assertRows(reader.visitContains(fieldRef, str("ab")).join());
        }
    }

    @Test
    public void testPartitionRotationAndGlobalRowIds() throws Exception {
        Options options = new Options();
        options.set(FMGlobalIndexOptions.PARTITION_ROW_COUNT, 2);
        options.set(FMGlobalIndexOptions.PARTITION_SIZE, MemorySize.ofKibiBytes(1));
        options.set(FMGlobalIndexOptions.SA_SAMPLE_RATE, 1);
        options.set(FMGlobalIndexOptions.COMPRESSION, "none");
        options.set(FMGlobalIndexOptions.LOCATE_COST_RATIO, 1d);
        indexer = new FMGlobalIndexer(dataField, options);

        List<GlobalIndexIOMeta> files =
                writeData(
                        Arrays.asList(
                                str("needle-0"),
                                str("other"),
                                str("needle-2"),
                                null,
                                str("needle-4")),
                        0);
        assertThat(files).hasSize(1);
        FMIndexFile.IndexMeta metadata = FMIndexFile.readIndexMeta(files.get(0).metadata());
        assertThat(metadata.rowCount).isEqualTo(5L);
        assertThat(metadata.partitions).hasSize(3);
        try (GlobalIndexReader reader = createReader(files, 5)) {
            assertRows(reader.visitContains(fieldRef, str("needle")).join(), 0L, 2L, 4L);
        }
    }

    @Test
    public void testFooterAndRankBlockCorruptionFailClosed() throws Exception {
        List<GlobalIndexIOMeta> files = writeData(Collections.singletonList(str("abcdef")), 0);
        GlobalIndexIOMeta file = files.get(0);
        corruptByte(file, file.fileSize() - FMIndexFile.CONTAINER_FOOTER_LENGTH + 8);
        try (GlobalIndexReader reader = createReader(files, 1)) {
            assertThatThrownBy(() -> reader.visitContains(fieldRef, str("abc")).join())
                    .isInstanceOf(CompletionException.class)
                    .hasMessageContaining("footer checksum mismatch");
        }

        files = writeData(Collections.singletonList(str("abcdef")), 0);
        file = files.get(0);
        corruptByte(file, 0);
        try (GlobalIndexReader reader = createReader(files, 1)) {
            assertThatThrownBy(() -> reader.visitContains(fieldRef, str("abc")).join())
                    .isInstanceOf(CompletionException.class)
                    .hasMessageContaining("block checksum mismatch");
        }
    }

    @Test
    public void testRandomizedExactnessAgainstByteScan() throws Exception {
        Options options = new Options();
        options.set(FMGlobalIndexOptions.PARTITION_ROW_COUNT, 17);
        options.set(FMGlobalIndexOptions.SA_SAMPLE_RATE, 1);
        options.set(FMGlobalIndexOptions.LOCATE_COST_RATIO, 1d);
        indexer = new FMGlobalIndexer(dataField, options);
        Random random = new Random(99173);
        List<BinaryString> values = new ArrayList<>();
        List<byte[]> rawValues = new ArrayList<>();
        for (int row = 0; row < 150; row++) {
            if (row % 19 == 0) {
                values.add(null);
                rawValues.add(null);
                continue;
            }
            byte[] value = new byte[random.nextInt(40)];
            random.nextBytes(value);
            values.add(BinaryString.fromBytes(value));
            rawValues.add(value);
        }
        List<GlobalIndexIOMeta> files = writeData(values, 0);
        try (GlobalIndexReader reader = createReader(files, values.size())) {
            for (int query = 0; query < 250; query++) {
                byte[] needle;
                if ((query & 1) == 0) {
                    byte[] source = rawValues.get(random.nextInt(rawValues.size()));
                    if (source == null || source.length == 0) {
                        needle = new byte[0];
                    } else {
                        int start = random.nextInt(source.length);
                        int end = start + random.nextInt(source.length - start + 1);
                        needle = Arrays.copyOfRange(source, start, end);
                    }
                } else {
                    needle = new byte[random.nextInt(8)];
                    random.nextBytes(needle);
                }
                List<Long> expected = new ArrayList<>();
                for (int row = 0; row < rawValues.size(); row++) {
                    if (rawValues.get(row) != null && contains(rawValues.get(row), needle)) {
                        expected.add((long) row);
                    }
                }
                assertRows(
                        reader.visitContains(fieldRef, BinaryString.fromBytes(needle)).join(),
                        expected.stream().mapToLong(Long::longValue).toArray());
            }
        }
    }

    @Test
    public void testNullPredicatesConjunctionAndCandidateIntersection() throws Exception {
        List<GlobalIndexIOMeta> files =
                writeData(
                        Arrays.asList(
                                str("alpha-beta"),
                                null,
                                str("alpha"),
                                str("beta"),
                                str("alpha-beta-tail")),
                        0);
        try (GlobalIndexReader reader = createReader(files, 5)) {
            assertRows(reader.visitIsNull(fieldRef).join(), 1L);
            assertRows(reader.visitIsNotNull(fieldRef).join(), 0L, 2L, 3L, 4L);
            org.apache.paimon.globalindex.ContainsRefiningGlobalIndexReader refining =
                    (org.apache.paimon.globalindex.ContainsRefiningGlobalIndexReader) reader;
            org.apache.paimon.utils.RoaringNavigableMap64 candidates =
                    new org.apache.paimon.utils.RoaringNavigableMap64();
            candidates.add(0L);
            candidates.add(2L);
            candidates.add(4L);
            assertRows(
                    refining.visitContainsConjunction(
                                    fieldRef,
                                    Arrays.asList(str("alpha"), str("beta")),
                                    GlobalIndexResult.create(candidates))
                            .join(),
                    0L,
                    4L);
        }
    }

    @Test
    public void testEmptyShardServiceLoadingAndFileCoverageValidation() throws Exception {
        FMGlobalIndexWriter writer = indexer.createWriter(fileWriter);
        assertThat(writer.finish()).isEmpty();
        try (GlobalIndexReader reader = createReader(Collections.emptyList(), 0)) {
            assertRows(reader.visitContains(fieldRef, str("")).join());
        }
        assertThat(GlobalIndexer.create("fmindex", dataField, new Options()))
                .isInstanceOf(FMGlobalIndexer.class);

        List<GlobalIndexIOMeta> shifted = writeData(Collections.singletonList(str("value")), 1);
        assertThatThrownBy(() -> createReader(shifted, 1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outside source range");
    }

    @Test
    public void testV1NoneGoldenFixtureIsStableAndReadable() throws Exception {
        Options options = new Options();
        options.set(FMGlobalIndexOptions.PARTITION_ROW_COUNT, 100);
        options.set(FMGlobalIndexOptions.SA_SAMPLE_RATE, 4);
        options.set(FMGlobalIndexOptions.COMPRESSION, "none");
        options.set(FMGlobalIndexOptions.LOCATE_COST_RATIO, 1d);
        indexer = new FMGlobalIndexer(dataField, options);
        List<GlobalIndexIOMeta> actualFiles =
                writeData(
                        Arrays.asList(
                                str("banana"),
                                null,
                                BinaryString.fromBytes(
                                        new byte[] {0, (byte) 0xFF, 'b', 'a', 'n', 'a', 'n', 'a'}),
                                str("")),
                        0);
        byte[] actual =
                Files.readAllBytes(
                        java.nio.file.Paths.get(actualFiles.get(0).filePath().toUri().getPath()));
        byte[] fixture =
                Base64.getMimeDecoder()
                        .decode(
                                new String(
                                                Files.readAllBytes(
                                                        java.nio.file.Paths.get(
                                                                Objects.requireNonNull(
                                                                                getClass()
                                                                                        .getResource(
                                                                                                "/fmindex-v1-golden.base64"))
                                                                        .toURI())),
                                                StandardCharsets.US_ASCII)
                                        .trim());
        assertThat(actual).containsExactly(fixture);

        java.nio.file.Path fixturePath = tempPath.resolve("fmindex-v1-golden.index");
        Files.write(fixturePath, fixture);
        GlobalIndexIOMeta fixtureMeta =
                new GlobalIndexIOMeta(
                        new Path(fixturePath.toUri()),
                        fixture.length,
                        actualFiles.get(0).metadata());
        try (GlobalIndexReader reader = createReader(Collections.singletonList(fixtureMeta), 4)) {
            assertRows(reader.visitContains(fieldRef, str("banana")).join(), 0L, 2L);
            assertRows(reader.visitContains(fieldRef, str("")).join(), 0L, 2L, 3L);
        }
    }

    @Test
    public void testV1ChecksumUsesIeeeCrc32Contract() {
        byte[] bytes = "123456789".getBytes(StandardCharsets.US_ASCII);
        assertThat(FMIndexFile.crc32(bytes, 0, bytes.length, BlockCompressionType.NONE))
                .isEqualTo(0x00C49E49);
    }

    @Test
    public void testDefaultLz4IsPersistedPerCompressibleBlock() throws Exception {
        String repeated = String.join("", Collections.nCopies(20_000, "compressible-value-"));
        List<GlobalIndexIOMeta> files = writeData(Collections.singletonList(str(repeated)), 0);
        GlobalIndexIOMeta file = files.get(0);
        try (org.apache.paimon.fs.SeekableInputStream input =
                fileIO.newInputStream(file.filePath())) {
            FMIndexFile.Footer footer = FMIndexFile.readFooter(input, file.fileSize());
            FMIndexFile.Directory directory =
                    FMIndexFile.readDirectory(input, footer, file.fileSize());
            assertThat(footer.directory.compressionId)
                    .isEqualTo(BlockCompressionType.LZ4.persistentId());
            assertThat(
                            Arrays.stream(directory.wavelets)
                                    .flatMap(vector -> vector.blocks.stream())
                                    .anyMatch(
                                            block ->
                                                    block.block.compressionId
                                                            == BlockCompressionType.LZ4
                                                                    .persistentId()))
                    .isTrue();
        }
        try (GlobalIndexReader reader = createReader(files, 1)) {
            assertRows(reader.visitContains(fieldRef, str("value-compressible")).join(), 0L);
        }
    }

    @Test
    public void testExactnessAcrossMultiplePhysicalRankBlocks() throws Exception {
        Options options = new Options();
        options.set(FMGlobalIndexOptions.PARTITION_SIZE, MemorySize.ofMebiBytes(1));
        options.set(FMGlobalIndexOptions.SA_SAMPLE_RATE, 32);
        options.set(FMGlobalIndexOptions.COMPRESSION, "none");
        indexer = new FMGlobalIndexer(dataField, options);
        byte[] value = new byte[FMIndexFile.QUAD_BLOCK_VALUES * 2 + 137];
        Random random = new Random(7193);
        random.nextBytes(value);
        byte[] needle =
                Arrays.copyOfRange(
                        value,
                        FMIndexFile.QUAD_BLOCK_VALUES - 31,
                        FMIndexFile.QUAD_BLOCK_VALUES + 37);
        byte[] missing = Arrays.copyOf(needle, needle.length);
        missing[missing.length - 1] ^= 0x5A;
        List<GlobalIndexIOMeta> files =
                writeData(Collections.singletonList(BinaryString.fromBytes(value)), 0);

        try (GlobalIndexReader reader = createReader(files, 1)) {
            assertRows(reader.visitContains(fieldRef, BinaryString.fromBytes(needle)).join(), 0L);
            assertRows(reader.visitContains(fieldRef, BinaryString.fromBytes(missing)).join());
        }

        GlobalIndexIOMeta file = files.get(0);
        try (SeekableInputStream input = fileIO.newInputStream(file.filePath())) {
            assertThat(input).isInstanceOf(VectoredReadable.class);
            FMIndexFile.Footer footer = FMIndexFile.readFooter(input, file.fileSize());
            FMIndexFile.Directory directory =
                    FMIndexFile.readDirectory(input, footer, file.fileSize());
            List<FMIndexFile.QuadBlockMeta> blocks = directory.wavelets[0].blocks;
            assertThat(blocks).hasSizeGreaterThanOrEqualTo(2);
            List<FMIndexFile.BlockInfo> blockInfos =
                    Arrays.asList(blocks.get(0).block, blocks.get(1).block);
            AtomicInteger preadCalls = new AtomicInteger();
            SeekableInputStream counting = new CountingVectoredInput(input, preadCalls);
            assertThat(FMIndexFile.readBlocks(counting, blockInfos, file.fileSize())).hasSize(2);
            assertThat(preadCalls).hasValue(1);
        }
    }

    @Test
    public void testDenseOccurrenceGuardDeclinesIndexEvaluation() throws Exception {
        Options options = new Options();
        options.set(FMGlobalIndexOptions.SA_SAMPLE_RATE, 4);
        options.set(FMGlobalIndexOptions.LOCATE_COST_RATIO, 1d);
        indexer = new FMGlobalIndexer(dataField, options);
        String repeated = String.join("", Collections.nCopies(10_000, "a"));
        List<GlobalIndexIOMeta> files =
                writeData(Arrays.asList(str(repeated + "b"), str(repeated)), 0);
        GlobalIndexIOMeta file = files.get(0);
        long sampleBlockOffset;
        try (org.apache.paimon.fs.SeekableInputStream input =
                fileIO.newInputStream(file.filePath())) {
            FMIndexFile.Footer footer = FMIndexFile.readFooter(input, file.fileSize());
            sampleBlockOffset =
                    FMIndexFile.readDirectory(input, footer, file.fileSize())
                            .sampleValues
                            .blocks
                            .get(0)
                            .block
                            .offset;
        }
        corruptByte(file, sampleBlockOffset);

        try (GlobalIndexReader reader = createReader(files, 2)) {
            // Dense intervals are left to the source scan, so the corrupted SA sample is not read.
            assertThat(reader.visitContains(fieldRef, str("a")).join()).isEmpty();
            assertThatThrownBy(() -> reader.visitContains(fieldRef, str("b")).join())
                    .isInstanceOf(CompletionException.class)
                    .hasMessageContaining("block checksum mismatch");
        }

        Options conservative = new Options();
        conservative.set(FMGlobalIndexOptions.SA_SAMPLE_RATE, 4);
        conservative.set(FMGlobalIndexOptions.LOCATE_COST_RATIO, 0.0001d);
        indexer = new FMGlobalIndexer(dataField, conservative);
        try (GlobalIndexReader reader = createReader(files, 2)) {
            assertThat(reader.visitContains(fieldRef, str("b")).join()).isEmpty();
        }
    }

    @Test
    public void testReadCostOptionsAreValidated() {
        Options invalidRatio = new Options();
        invalidRatio.set(FMGlobalIndexOptions.LOCATE_COST_RATIO, 0d);
        assertThatThrownBy(() -> new FMGlobalIndexer(dataField, invalidRatio))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("locate cost ratio");

        Options invalidPageSize = new Options();
        invalidPageSize.set(FMGlobalIndexOptions.DEMAND_PAGE_SIZE, MemorySize.ofKibiBytes(32));
        assertThatThrownBy(() -> new FMGlobalIndexer(dataField, invalidPageSize))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("demand page size");
    }

    @Test
    public void testZeroReadCacheDeclinesLocate() throws Exception {
        Options options = new Options();
        options.set(FMGlobalIndexOptions.PARTITION_ROW_COUNT, 10);
        options.set(FMGlobalIndexOptions.SA_SAMPLE_RATE, 1024);
        options.set(FMGlobalIndexOptions.COMPRESSION, "none");
        options.set(FMGlobalIndexOptions.READ_CACHE_SIZE, MemorySize.ofBytes(0));
        options.set(FMGlobalIndexOptions.LOCATE_COST_RATIO, 1d);
        indexer = new FMGlobalIndexer(dataField, options);
        String value = String.join("", Collections.nCopies(3_000, "x")) + "unique-needle";
        List<GlobalIndexIOMeta> files = writeData(Collections.singletonList(str(value)), 0);

        try (GlobalIndexReader reader = createReader(files, 1)) {
            assertThat(reader.visitContains(fieldRef, str("unique-needle")).join()).isEmpty();
        }
    }

    @Test
    public void testZeroReadCacheScansNullBitmapOncePerBlock() throws Exception {
        Options options = new Options();
        options.set(FMGlobalIndexOptions.PARTITION_ROW_COUNT, 1_000);
        options.set(FMGlobalIndexOptions.COMPRESSION, "none");
        options.set(FMGlobalIndexOptions.READ_CACHE_SIZE, MemorySize.ofBytes(0));
        indexer = new FMGlobalIndexer(dataField, options);
        List<BinaryString> values = new ArrayList<>();
        for (int row = 0; row < 1_000; row++) {
            values.add((row & 1) == 0 ? null : str("value-" + row));
        }
        List<GlobalIndexIOMeta> files = writeData(values, 0);

        AtomicInteger preadCalls = new AtomicInteger();
        fileReader =
                meta ->
                        new CountingVectoredInput(
                                fileIO.newInputStream(meta.filePath()), preadCalls);
        try (GlobalIndexReader reader = createReader(files, values.size())) {
            assertRows(
                    reader.visitIsNull(fieldRef).join(),
                    java.util.stream.LongStream.range(0, values.size())
                            .filter(row -> (row & 1) == 0)
                            .toArray());
        }
        assertThat(preadCalls.get()).isLessThan(20);
    }

    @Test
    public void testDenseQueriesDeclineWithoutStoredValues() throws Exception {
        Options options = new Options();
        options.set(FMGlobalIndexOptions.PARTITION_ROW_COUNT, 10);
        options.set(FMGlobalIndexOptions.SA_SAMPLE_RATE, 4);
        options.set(FMGlobalIndexOptions.COMPRESSION, "none");
        options.set(FMGlobalIndexOptions.LOCATE_COST_RATIO, 1d);
        indexer = new FMGlobalIndexer(dataField, options);
        List<GlobalIndexIOMeta> files = writeData(Arrays.asList(str("aaaa"), null, str("bbbb")), 0);

        try (GlobalIndexReader reader = createReader(files, 3)) {
            assertThat(reader.visitContains(fieldRef, str("a")).join()).isEmpty();
            assertRows(reader.visitContains(fieldRef, str("missing")).join());
            assertRows(reader.visitContains(fieldRef, str("")).join(), 0L, 2L);
            org.apache.paimon.utils.RoaringNavigableMap64 candidates =
                    new org.apache.paimon.utils.RoaringNavigableMap64();
            candidates.add(2L);
            assertRows(
                    ((org.apache.paimon.globalindex.ContainsRefiningGlobalIndexReader) reader)
                            .visitContainsConjunction(
                                    fieldRef,
                                    Collections.singletonList(str("bbbb")),
                                    GlobalIndexResult.create(candidates))
                            .join(),
                    2L);
        }
    }

    @Test
    public void testDenseCandidateQueryDeclinesIndexEvaluation() throws Exception {
        Options options = new Options();
        options.set(FMGlobalIndexOptions.PARTITION_ROW_COUNT, 1_000);
        options.set(FMGlobalIndexOptions.SA_SAMPLE_RATE, 4);
        options.set(FMGlobalIndexOptions.COMPRESSION, "none");
        indexer = new FMGlobalIndexer(dataField, options);
        List<GlobalIndexIOMeta> files =
                writeData(new ArrayList<>(Collections.nCopies(200, str("aaaaa"))), 0);

        org.apache.paimon.utils.RoaringNavigableMap64 candidates =
                new org.apache.paimon.utils.RoaringNavigableMap64();
        candidates.add(17L);
        try (GlobalIndexReader reader = createReader(files, 200)) {
            org.apache.paimon.globalindex.ContainsRefiningGlobalIndexReader refining =
                    (org.apache.paimon.globalindex.ContainsRefiningGlobalIndexReader) reader;
            assertThat(
                            refining.visitContainsConjunction(
                                            fieldRef,
                                            Collections.singletonList(str("a")),
                                            GlobalIndexResult.create(candidates))
                                    .join())
                    .isEmpty();
        }
    }

    @Test
    public void testMediumOccurrenceGuardDeclinesIndexEvaluation() throws Exception {
        Options options = new Options();
        options.set(FMGlobalIndexOptions.PARTITION_ROW_COUNT, 1_000);
        options.set(FMGlobalIndexOptions.SA_SAMPLE_RATE, 32);
        options.set(FMGlobalIndexOptions.COMPRESSION, "none");
        indexer = new FMGlobalIndexer(dataField, options);
        List<GlobalIndexIOMeta> files =
                writeData(new ArrayList<>(Collections.nCopies(200, str("aaaaa"))), 0);

        try (GlobalIndexReader reader = createReader(files, 200)) {
            assertThat(reader.visitContains(fieldRef, str("a")).join()).isEmpty();
        }
    }

    @Test
    public void testCandidatePartitionPruningSkipsUnrelatedWaveletData() throws Exception {
        Options options = new Options();
        options.set(FMGlobalIndexOptions.PARTITION_ROW_COUNT, 2);
        options.set(FMGlobalIndexOptions.SA_SAMPLE_RATE, 1);
        options.set(FMGlobalIndexOptions.COMPRESSION, "none");
        options.set(FMGlobalIndexOptions.LOCATE_COST_RATIO, 1d);
        indexer = new FMGlobalIndexer(dataField, options);
        List<GlobalIndexIOMeta> files =
                writeData(
                        Arrays.asList(
                                str("needle-0"),
                                str("other-1"),
                                str("needle-2"),
                                str("other-3"),
                                str("needle-4")),
                        0);
        GlobalIndexIOMeta file = files.get(0);
        FMIndexFile.IndexMeta indexMeta = FMIndexFile.readIndexMeta(file.metadata());
        assertThat(indexMeta.partitions).hasSize(3);
        long unrelatedWaveletOffset;
        try (org.apache.paimon.fs.SeekableInputStream input =
                fileIO.newInputStream(file.filePath())) {
            FMIndexFile.Footer footer =
                    FMIndexFile.readFooter(input, indexMeta.partitions.get(0), file.fileSize());
            unrelatedWaveletOffset =
                    FMIndexFile.readDirectory(input, footer, file.fileSize())
                            .wavelets[0]
                            .blocks
                            .get(0)
                            .block
                            .offset;
        }
        corruptByte(file, unrelatedWaveletOffset);

        org.apache.paimon.utils.RoaringNavigableMap64 candidates =
                new org.apache.paimon.utils.RoaringNavigableMap64();
        candidates.add(4L);
        CountingDirectExecutor executor = new CountingDirectExecutor();
        try (GlobalIndexReader reader = indexer.createReader(fileReader, files, 5, executor)) {
            org.apache.paimon.globalindex.ContainsRefiningGlobalIndexReader refining =
                    (org.apache.paimon.globalindex.ContainsRefiningGlobalIndexReader) reader;
            assertRows(
                    refining.visitContainsConjunction(
                                    fieldRef,
                                    Collections.singletonList(str("needle")),
                                    GlobalIndexResult.create(candidates))
                            .join(),
                    4L);
        }
        assertThat(executor.submittedTasks).hasValue(1);
    }

    @Test
    public void testManifestPartitionMetadataMismatchFailsClosed() throws Exception {
        Options options = new Options();
        options.set(FMGlobalIndexOptions.PARTITION_ROW_COUNT, 2);
        options.set(FMGlobalIndexOptions.COMPRESSION, "none");
        indexer = new FMGlobalIndexer(dataField, options);
        GlobalIndexIOMeta first =
                writeData(Arrays.asList(str("needle-0"), str("other-1")), 0).get(0);
        GlobalIndexIOMeta second =
                writeData(Arrays.asList(str("needle-2"), str("other-3")), 2).get(0);
        List<GlobalIndexIOMeta> swapped =
                Arrays.asList(
                        new GlobalIndexIOMeta(
                                first.filePath(), first.fileSize(), second.metadata()),
                        new GlobalIndexIOMeta(
                                second.filePath(), second.fileSize(), first.metadata()));
        org.apache.paimon.utils.RoaringNavigableMap64 candidates =
                new org.apache.paimon.utils.RoaringNavigableMap64();
        candidates.add(0L);

        try (GlobalIndexReader reader = createReader(swapped, 4)) {
            org.apache.paimon.globalindex.ContainsRefiningGlobalIndexReader refining =
                    (org.apache.paimon.globalindex.ContainsRefiningGlobalIndexReader) reader;
            assertThatThrownBy(
                            () ->
                                    refining.visitContainsConjunction(
                                                    fieldRef,
                                                    Collections.singletonList(str("needle")),
                                                    GlobalIndexResult.create(candidates))
                                            .join())
                    .isInstanceOf(CompletionException.class)
                    .hasMessageContaining("does not match the container directory");
        }
    }

    @Test
    public void testManifestPartitionMetadataChecksumFailsClosed() throws Exception {
        Options options = new Options();
        options.set(FMGlobalIndexOptions.COMPRESSION, "none");
        indexer = new FMGlobalIndexer(dataField, options);
        List<GlobalIndexIOMeta> files = writeData(Collections.singletonList(str("needle")), 0);
        GlobalIndexIOMeta file = files.get(0);
        byte[] corruptedMetadata = Arrays.copyOf(file.metadata(), file.metadata().length);
        corruptedMetadata[corruptedMetadata.length - 1] ^= 1;
        GlobalIndexIOMeta corrupted =
                new GlobalIndexIOMeta(file.filePath(), file.fileSize(), corruptedMetadata);

        assertThatThrownBy(() -> createReader(Collections.singletonList(corrupted), 1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("metadata checksum mismatch");
    }

    @Test
    public void testDemandPagingDoesNotOutgrowReadCache() throws Exception {
        Options options = new Options();
        options.set(FMGlobalIndexOptions.PARTITION_SIZE, MemorySize.ofMebiBytes(1));
        options.set(FMGlobalIndexOptions.SA_SAMPLE_RATE, 32);
        options.set(FMGlobalIndexOptions.COMPRESSION, "none");
        options.set(FMGlobalIndexOptions.READ_CACHE_SIZE, MemorySize.ofKibiBytes(64));
        options.set(FMGlobalIndexOptions.DEMAND_PAGE_SIZE, MemorySize.ofKibiBytes(512));
        indexer = new FMGlobalIndexer(dataField, options);
        byte[] value = new byte[FMIndexFile.QUAD_BLOCK_VALUES * 3 + 137];
        new Random(817_331).nextBytes(value);
        byte[] needle = Arrays.copyOfRange(value, 177_777, 177_905);
        List<GlobalIndexIOMeta> files =
                writeData(Collections.singletonList(BinaryString.fromBytes(value)), 0);

        AtomicLong maximumPread = new AtomicLong();
        fileReader =
                meta ->
                        new CountingVectoredInput(
                                fileIO.newInputStream(meta.filePath()),
                                new AtomicInteger(),
                                maximumPread);
        try (GlobalIndexReader reader = createReader(files, 1)) {
            assertRows(reader.visitContains(fieldRef, BinaryString.fromBytes(needle)).join(), 0L);
        }
        assertThat(maximumPread.get()).isLessThanOrEqualTo(MemorySize.ofKibiBytes(64).getBytes());
    }

    private List<GlobalIndexIOMeta> writeData(List<BinaryString> values, long firstRowId)
            throws Exception {
        FMGlobalIndexWriter writer = indexer.createWriter(fileWriter);
        for (int i = 0; i < values.size(); i++) {
            writer.write(values.get(i), firstRowId + i);
        }
        return toIOMetas(writer.finish());
    }

    private List<GlobalIndexIOMeta> toIOMetas(List<ResultEntry> entries) throws IOException {
        List<GlobalIndexIOMeta> result = new ArrayList<>();
        for (ResultEntry entry : entries) {
            Path path = new Path(basePath, entry.fileName());
            result.add(new GlobalIndexIOMeta(path, fileIO.getFileSize(path), entry.meta()));
        }
        return result;
    }

    private GlobalIndexReader createReader(List<GlobalIndexIOMeta> files, long totalRows) {
        return indexer.createReader(fileReader, files, totalRows, newDirectExecutorService());
    }

    private static final class CountingDirectExecutor extends AbstractExecutorService {
        private final AtomicInteger submittedTasks = new AtomicInteger();
        private boolean shutdown;

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdown = true;
            return Collections.emptyList();
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            return shutdown;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return shutdown;
        }

        @Override
        public void execute(Runnable command) {
            submittedTasks.incrementAndGet();
            command.run();
        }
    }

    private void corruptByte(GlobalIndexIOMeta meta, long offset) throws IOException {
        try (RandomAccessFile file =
                new RandomAccessFile(meta.filePath().toUri().getPath(), "rw")) {
            file.seek(offset);
            int value = file.read();
            file.seek(offset);
            file.write(value ^ 0x5A);
        }
    }

    private static void assertRows(
            Optional<org.apache.paimon.globalindex.GlobalIndexResult> result, long... rows) {
        assertThat(result).isPresent();
        List<Long> actual = new ArrayList<>();
        for (long row : result.get().results()) {
            actual.add(row);
        }
        assertThat(actual).containsExactly(Arrays.stream(rows).boxed().toArray(Long[]::new));
    }

    private static BinaryString str(String value) {
        return BinaryString.fromString(value);
    }

    private static boolean contains(byte[] value, byte[] needle) {
        if (needle.length == 0) {
            return true;
        }
        for (int start = 0; start <= value.length - needle.length; start++) {
            int position = 0;
            while (position < needle.length && value[start + position] == needle[position]) {
                position++;
            }
            if (position == needle.length) {
                return true;
            }
        }
        return false;
    }

    private static final class CountingVectoredInput extends SeekableInputStreamWrapper
            implements VectoredReadable {

        private final VectoredReadable vectored;
        private final AtomicInteger preadCalls;
        private final AtomicLong maximumPread;

        private CountingVectoredInput(SeekableInputStream input, AtomicInteger preadCalls) {
            this(input, preadCalls, new AtomicLong());
        }

        private CountingVectoredInput(
                SeekableInputStream input, AtomicInteger preadCalls, AtomicLong maximumPread) {
            super(input);
            this.vectored = (VectoredReadable) input;
            this.preadCalls = preadCalls;
            this.maximumPread = maximumPread;
        }

        @Override
        public int pread(long position, byte[] buffer, int offset, int length) throws IOException {
            preadCalls.incrementAndGet();
            maximumPread.accumulateAndGet(length, Math::max);
            return vectored.pread(position, buffer, offset, length);
        }
    }
}
