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

package org.apache.paimon.manifest;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.Segments;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.SeekableInputStreamWrapper;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.operation.metrics.CacheMetrics;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RowRangeIndex;
import org.apache.paimon.utils.SegmentsCache;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.TestKeyValueGenerator.DEFAULT_PART_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Cross-path correctness, isolation and bounded-memory tests for decoded manifest blocks. */
class ManifestEntryCacheTest {

    @TempDir java.nio.file.Path temp;

    @Test
    void partialFullAndCheckedReadsReuseTheSameEntries() throws Exception {
        Fixture f = new Fixture(4000);
        ManifestSidecar.Selection partial = f.selection(1, 3);
        CacheMetrics metrics = new CacheMetrics();
        ManifestFile reader = f.reader(f.cache).withCacheMetrics(metrics);
        assertThat(f.read(reader, partial)).containsExactlyElementsOf(f.expected(partial));
        assertThat(f.cache.getIfPresents(f.path)).isNull();
        ManifestSidecar.Block block = partial.blocks().get(0);
        Segments pinned = f.cached(block);
        assertThat(pinned).isInstanceOf(ManifestEntrySegments.class);

        // Without a complete directory, the first full read discovers the physical blocks.
        assertThat(reader.readWithIOException(f.meta.fileName(), f.meta.fileSize()))
                .containsExactlyElementsOf(f.entries);
        assertThat(metrics.getHitObject().get()).isGreaterThanOrEqualTo(partial.blocks().size());
        assertThat(f.cached(block)).isSameAs(pinned);
        assertThat(f.cache.getIfPresents(f.path))
                .isNotNull()
                .isNotInstanceOf(ManifestEntrySegments.class);

        f.io.reset();
        f.io.rejectReads = true;
        assertThat(f.read(f.reader(f.cache), partial))
                .containsExactlyElementsOf(f.expected(partial));
        assertThat(f.reader(f.cache).read(f.meta.fileName())).containsExactlyElementsOf(f.entries);
        assertThat(f.reader(f.cache).readWithIOException(f.meta.fileName()))
                .containsExactlyElementsOf(f.entries);
        assertThat(f.io.opened).isEmpty();
    }

    @Test
    void fullReadWarmsSubsequentSelectedRead() {
        Fixture f = new Fixture(2000);
        assertThat(f.reader(f.cache).read(f.meta.fileName())).containsExactlyElementsOf(f.entries);
        f.io.reset();
        f.io.rejectReads = true;
        ManifestSidecar.Selection selected = f.selection(0, 2);
        assertThat(f.read(f.reader(f.cache), selected))
                .containsExactlyElementsOf(f.expected(selected));
        assertThat(f.io.opened).isEmpty();
    }

    @Test
    void differentPredicatesAndConvertersNeverCachePartialEntries() {
        Fixture f = new Fixture(1500);
        List<BinaryRow> partitions =
                f.entries.stream()
                        .map(ManifestEntry::partition)
                        .distinct()
                        .collect(Collectors.toList());
        assertThat(partitions.size()).isGreaterThan(1);
        ManifestFile reader = f.reader(f.cache);
        for (int i = 0; i < 2; i++) {
            PartitionPredicate partition =
                    PartitionPredicate.fromMultiple(
                            DEFAULT_PART_TYPE, Collections.singletonList(partitions.get(i)));
            int bucket =
                    f.entries.stream()
                            .filter(e -> partition.test(e.partition()))
                            .findFirst()
                            .get()
                            .bucket();
            BucketFilter buckets = new BucketFilter(false, bucket, null, null);
            FileKind kind = i == 0 ? FileKind.ADD : FileKind.DELETE;
            Filter<ManifestEntry> predicate = e -> e.kind() == kind;
            Function<ManifestEntry, ManifestEntry> convertor =
                    i == 0 ? ManifestEntry::copyWithoutStats : Function.identity();
            List<ManifestEntry> expected =
                    f.entries.stream()
                            .filter(e -> partition.test(e.partition()))
                            .filter(e -> buckets.test(e.partition(), e.bucket(), e.totalBuckets()))
                            .filter(predicate::test)
                            .map(convertor)
                            .collect(Collectors.toList());
            assertThat(
                            reader.read(
                                    f.meta.fileName(),
                                    f.meta.fileSize(),
                                    partition,
                                    buckets,
                                    row -> true,
                                    predicate,
                                    convertor,
                                    f.all))
                    .containsExactlyElementsOf(expected);
            f.io.reset();
            f.io.rejectReads = true;
        }
        assertThat(reader.read(f.meta.fileName())).containsExactlyElementsOf(f.entries);
        assertThat(f.io.opened).isEmpty();
    }

    @Test
    void mixedHitsAndMissesReadOnlyMissingRangesAndCoalesceNeighbours() {
        Fixture f = new Fixture(4000);
        ManifestFile reader = f.reader(f.cache);
        f.read(reader, f.selection(1));
        f.io.reset();
        ManifestSidecar.Selection mixed = f.selection(0, 1, 2);
        assertThat(f.read(reader, mixed)).containsExactlyElementsOf(f.expected(mixed));
        assertThat(f.io.seeks)
                .containsExactly(f.all.blocks().get(0).offset, f.all.blocks().get(2).offset);
        assertThat(f.io.bytes.get())
                .isEqualTo(f.all.blocks().get(0).length + f.all.blocks().get(2).length);
        assertThat(f.io.opened).containsExactly(f.path);
        f.io.reset();
        ManifestSidecar.Selection adjacent = f.selection(3, 4);
        assertThat(f.read(reader, adjacent)).containsExactlyElementsOf(f.expected(adjacent));
        assertThat(f.io.seeks).containsExactly(f.all.blocks().get(3).offset);
        assertThat(f.io.bytes.get())
                .isEqualTo(f.all.blocks().get(3).length + f.all.blocks().get(4).length);
    }

    @Test
    void truncatedBlockNeverPublishesDecodedEntriesOrCompleteDirectory() throws Exception {
        Fixture f = new Fixture(1000);
        ManifestSidecar.Selection selected = f.selection(1);
        ManifestSidecar.Block block = selected.blocks().get(0);
        java.nio.file.Path file = java.nio.file.Paths.get(f.path.toUri().getPath());
        byte[] original = Files.readAllBytes(file);
        Files.write(file, Arrays.copyOf(original, (int) (block.offset + block.length - 1)));
        f.io.reset();
        assertThatThrownBy(() -> f.read(f.reader(f.cache), selected))
                .hasRootCauseInstanceOf(EOFException.class);
        assertThat(f.cached(block)).isNull();
        assertThat(f.cache.getIfPresents(f.path)).isNull();
        assertThat(f.io.closed.get()).isEqualTo(f.io.opened.size());
        Files.write(file, original);
        assertThat(f.read(f.reader(f.cache), selected))
                .containsExactlyElementsOf(f.expected(selected));
        assertThat(f.cached(block)).isInstanceOf(ManifestEntrySegments.class);
    }

    @Test
    void decodedMemoryLimitFallsBackWithoutCachingAPrefixOrReadingTwice() {
        Fixture f = new Fixture(1500);
        SegmentsCache<Path> small = cache(8192, Long.MAX_VALUE);
        ManifestFile reader = f.reader(small);
        ManifestSidecar.Selection selected = f.selection(0, 1);
        long bytes = selected.blocks().stream().mapToLong(b -> b.length).sum();
        for (int round = 0; round < 2; round++) {
            f.io.reset();
            assertThat(f.read(reader, selected)).containsExactlyElementsOf(f.expected(selected));
            assertThat(f.io.bytes.get()).isEqualTo(bytes);
            for (ManifestSidecar.Block block : selected.blocks()) {
                assertThat(
                                blockCache(small)
                                        .getIfPresents(
                                                new BlockKey(f.path, block.offset, block.length)))
                        .isNull();
            }
            assertThat(small.totalCacheBytes()).isLessThanOrEqualTo(8192);
        }
    }

    @Test
    void evictionStaysWithinBudgetAndReloadsOnlyTheMissingBlocks() {
        Fixture f = new Fixture(4000);
        ManifestSidecar.Selection selected = f.selection(0, 1, 2, 3, 4);
        f.read(f.reader(f.cache), selected);
        long largest =
                selected.blocks().stream()
                        .mapToLong(b -> f.cached(b).totalMemorySize())
                        .max()
                        .getAsLong();
        long budget = largest * 2 + 3000;
        SegmentsCache<Path> small = cache(budget, Long.MAX_VALUE);
        ManifestFile reader = f.reader(small);
        assertThat(f.read(reader, selected)).containsExactlyElementsOf(f.expected(selected));
        assertThat(small.totalCacheBytes()).isLessThanOrEqualTo(budget);
        List<ManifestSidecar.Block> missing =
                selected.blocks().stream()
                        .filter(
                                b ->
                                        blockCache(small)
                                                        .getIfPresents(
                                                                new BlockKey(
                                                                        f.path, b.offset, b.length))
                                                == null)
                        .collect(Collectors.toList());
        assertThat(missing).isNotEmpty().hasSizeLessThan(selected.blocks().size());
        f.io.reset();
        assertThat(f.read(reader, selected)).containsExactlyElementsOf(f.expected(selected));
        assertThat(f.io.bytes.get()).isEqualTo(missing.stream().mapToLong(b -> b.length).sum());
        assertThat(small.totalCacheBytes()).isLessThanOrEqualTo(budget);
    }

    @Test
    void concurrentQueriesHaveIndependentReadersAndCursors() throws Exception {
        Fixture f = new Fixture(4000);
        ManifestFile shared = f.reader(f.cache);
        ExecutorService executor = Executors.newFixedThreadPool(6);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int task = 0; task < 12; task++) {
                final int block = task % 6;
                futures.add(
                        executor.submit(
                                () -> {
                                    ManifestSidecar.Selection selected =
                                            f.selection(block, block + 1);
                                    for (int round = 0; round < 10; round++) {
                                        assertThat(f.read(shared, selected))
                                                .containsExactlyElementsOf(f.expected(selected));
                                    }
                                }));
            }
            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void blockIdentityIncludesPathOffsetAndLength() {
        Path a = new Path("file:///a/manifest-1");
        Path b = new Path("file:///b/manifest-1");
        BlockKey key = new BlockKey(a, 100, 200);
        assertThat(key).isEqualTo(new BlockKey(a, 100, 200));
        assertThat(key.hashCode()).isEqualTo(new BlockKey(a, 100, 200).hashCode());
        assertThat(key)
                .isNotEqualTo(new BlockKey(b, 100, 200))
                .isNotEqualTo(new BlockKey(a, 101, 200))
                .isNotEqualTo(new BlockKey(a, 100, 201));
    }

    @SuppressWarnings("unchecked")
    private static SegmentsCache<BlockKey> blockCache(SegmentsCache<Path> cache) {
        return (SegmentsCache<BlockKey>) (SegmentsCache<?>) cache;
    }

    private static SegmentsCache<Path> cache(long memory, long element) {
        return new SegmentsCache<>(1024, MemorySize.ofBytes(memory), element, null, false);
    }

    private class Fixture {

        private final RecordingIO io = new RecordingIO();
        private final SegmentsCache<Path> cache = cache(64L << 20, 1L << 20);
        private final SegmentsCache<Path> sidecars = cache(16L << 20, 1L << 20);
        private final List<ManifestEntry> entries = new ArrayList<>();
        private final Path root = new Path(temp.toString());
        private final ManifestFileMeta meta;
        private final Path path;
        private final ManifestSidecar.Selection all;

        private Fixture(int count) {
            ManifestTestDataGenerator generator = ManifestTestDataGenerator.builder().build();
            for (int i = 0; i < count; i++) {
                ManifestEntry e = generator.next();
                entries.add(
                        ManifestEntry.create(
                                i % 3 == 0 ? FileKind.DELETE : FileKind.ADD,
                                e.partition(),
                                e.bucket(),
                                e.totalBuckets(),
                                e.file().newFirstRowId(i * 1000000L)));
            }
            ManifestFile reader = reader(cache);
            meta = reader.write(entries).get(0);
            path = new Path(root, "manifest/" + meta.fileName());
            all =
                    reader.selectBlocks(
                            meta,
                            RowRangeIndex.create(
                                    Collections.singletonList(new Range(0, Long.MAX_VALUE))));
            assertThat(all.blocks()).hasSizeGreaterThan(2);
            io.reset();
        }

        private ManifestFile reader(SegmentsCache<Path> bodyCache) {
            Options options = new Options();
            options.set(CoreOptions.DATA_EVOLUTION_ENABLED, true);
            options.set(CoreOptions.MANIFEST_SIDECAR_ENABLED, true);
            FileStorePathFactory paths =
                    new FileStorePathFactory(
                            root,
                            DEFAULT_PART_TYPE,
                            "default",
                            CoreOptions.FILE_FORMAT.defaultValue(),
                            CoreOptions.DATA_FILE_PREFIX.defaultValue(),
                            CoreOptions.CHANGELOG_FILE_PREFIX.defaultValue(),
                            CoreOptions.PARTITION_GENERATE_LEGACY_NAME.defaultValue(),
                            CoreOptions.FILE_SUFFIX_INCLUDE_COMPRESSION.defaultValue(),
                            CoreOptions.FILE_COMPRESSION.defaultValue(),
                            null,
                            null,
                            CoreOptions.ExternalPathStrategy.NONE,
                            null,
                            false,
                            null);
            return new ManifestFile.Factory(
                            io,
                            new FileSystemSchemaManager(io, root),
                            DEFAULT_PART_TYPE,
                            FileFormat.fromIdentifier("avro", new Options()),
                            "zstd",
                            paths,
                            Long.MAX_VALUE,
                            bodyCache,
                            sidecars,
                            new CoreOptions(options))
                    .create();
        }

        private ManifestSidecar.Selection selection(int... indices) {
            List<ManifestSidecar.Block> blocks = new ArrayList<>();
            for (int index : indices) {
                blocks.add(all.blocks().get(index));
            }
            return new ManifestSidecar.Selection(all.header(), blocks);
        }

        private List<ManifestEntry> expected(ManifestSidecar.Selection selected) {
            List<ManifestEntry> result = new ArrayList<>();
            for (ManifestSidecar.Block b : selected.blocks()) {
                result.addAll(
                        entries.subList(
                                (int) b.firstRecord, (int) (b.firstRecord + b.recordCount)));
            }
            return result;
        }

        private List<ManifestEntry> read(ManifestFile reader, ManifestSidecar.Selection selected) {
            return reader.read(
                    meta.fileName(),
                    meta.fileSize(),
                    null,
                    null,
                    row -> true,
                    e -> true,
                    Function.identity(),
                    selected);
        }

        private Segments cached(ManifestSidecar.Block block) {
            return blockCache(cache).getIfPresents(new BlockKey(path, block.offset, block.length));
        }
    }

    private static final class RecordingIO extends LocalFileIO {

        private final List<Path> opened = Collections.synchronizedList(new ArrayList<>());
        private final List<Long> seeks = Collections.synchronizedList(new ArrayList<>());
        private final AtomicLong bytes = new AtomicLong();
        private final AtomicInteger closed = new AtomicInteger();
        private volatile boolean rejectReads;

        private void reset() {
            opened.clear();
            seeks.clear();
            bytes.set(0);
            closed.set(0);
        }

        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            if (rejectReads) {
                throw new IOException("Unexpected file read: " + path);
            }
            opened.add(path);
            return new SeekableInputStreamWrapper(super.newInputStream(path)) {

                @Override
                public int read() throws IOException {
                    int value = super.read();
                    if (value >= 0) {
                        bytes.incrementAndGet();
                    }
                    return value;
                }

                @Override
                public int read(byte[] b, int off, int len) throws IOException {
                    int count = super.read(b, off, len);
                    if (count > 0) {
                        bytes.addAndGet(count);
                    }
                    return count;
                }

                @Override
                public void seek(long offset) throws IOException {
                    seeks.add(offset);
                    super.seek(offset);
                }

                @Override
                public void close() throws IOException {
                    closed.incrementAndGet();
                    super.close();
                }
            };
        }
    }
}
