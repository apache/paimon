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

package org.apache.paimon.fs.cache;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileRange;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.VectoredReadUtils;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.FileType;
import org.apache.paimon.utils.InstantiationUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.apache.paimon.options.CatalogOptions.LOCAL_CACHE_DIR;
import static org.apache.paimon.options.CatalogOptions.LOCAL_CACHE_ENABLED;
import static org.apache.paimon.options.CatalogOptions.LOCAL_CACHE_MAX_SIZE;
import static org.apache.paimon.options.CatalogOptions.LOCAL_CACHE_WHITELIST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link CachingFileIO} and {@link CachingSeekableInputStream}. */
class CachingFileIOTest {

    private static final int VECTOR_RANGES = 8;
    private static final int VECTOR_LENGTH = 256;
    private static final int VECTOR_STRIDE = 32 * 1024;
    // BlockingExecutor has exactly this many permits; below VECTOR_RANGES, readVectored would block
    // the calling thread, which is also the only thread that can release the open gate
    private static final int VECTOR_PARALLELISM = 32;

    static {
        if (VECTOR_PARALLELISM < VECTOR_RANGES) {
            throw new AssertionError("VECTOR_PARALLELISM must be at least VECTOR_RANGES");
        }
    }

    @TempDir java.nio.file.Path tempDir;

    private String cacheDir;

    @BeforeEach
    void setUp() {
        cacheDir = tempDir.resolve("cache").toString();
        MockFileIO.resetGlobalInputStreamCalls();
    }

    @Test
    void testMemoryModeServesFreshContentAfterInPlaceOverwrite() throws IOException {
        MockFileIO delegate = new MockFileIO();
        CachingFileIO cachingIO =
                newCachingFileIO(
                        delegate,
                        new LocalMemoryCacheManager(Long.MAX_VALUE, 64),
                        EnumSet.of(FileType.META),
                        64);
        Path consumer = new Path("consumer-1");

        // Same path overwritten in place with new content and a new mtime; the
        // memory cache must not keep serving the first version's blocks.
        delegate.addFile("consumer-1", "v1cc".getBytes(), 1000L);
        try (SeekableInputStream in = cachingIO.newInputStream(consumer)) {
            byte[] buf = new byte[4];
            in.read(buf, 0, 4);
            assertThat(new String(buf)).isEqualTo("v1cc");
        }
        // one remote open, after which the first version's blocks are cached
        assertThat(delegate.newInputStreamCallCount("consumer-1")).isEqualTo(1);

        delegate.addFile("consumer-1", "v2cc".getBytes(), 2000L);
        try (SeekableInputStream in = cachingIO.newInputStream(consumer)) {
            byte[] buf = new byte[4];
            in.read(buf, 0, 4);
            assertThat(new String(buf)).isEqualTo("v2cc");
        }
        // the new version has a different key, forcing a fresh remote read
        assertThat(delegate.newInputStreamCallCount("consumer-1")).isEqualTo(2);
    }

    @Test
    void testCreateBlobPresignedUrlDelegates() throws IOException {
        FileIO delegate = mock(FileIO.class);
        CachingFileIO cachingIO =
                newCachingFileIO(
                        delegate,
                        new LocalMemoryCacheManager(1024, 64),
                        EnumSet.of(FileType.DATA),
                        64);
        Path tableRoot = new Path("oss://bucket/table");
        BlobDescriptor descriptor =
                new BlobDescriptor("oss://bucket/table/bucket-0/data.blob", 0, 1);
        Duration validity = Duration.ofMinutes(5);
        when(delegate.createBlobPresignedUrl(tableRoot, descriptor, validity))
                .thenReturn("https://example");

        assertThat(cachingIO.createBlobPresignedUrl(tableRoot, descriptor, validity))
                .isEqualTo("https://example");
        verify(delegate).createBlobPresignedUrl(tableRoot, descriptor, validity);
    }

    @Test
    void testTryToWriteAtomicReachesDelegateOverride() throws IOException {
        FileIO delegate = mock(FileIO.class);
        CachingFileIO cachingIO =
                newCachingFileIO(
                        delegate,
                        new LocalMemoryCacheManager(1024, 64),
                        EnumSet.of(FileType.DATA),
                        64);
        Path target = new Path("oss://bucket/table/snapshot/LATEST");
        when(delegate.tryToWriteAtomic(target, "content")).thenReturn(true);

        assertThat(cachingIO.tryToWriteAtomic(target, "content")).isTrue();
        verify(delegate).tryToWriteAtomic(target, "content");
        // the interface default would have written a temp file and renamed it instead
        verify(delegate, never()).rename(any(), any());
    }

    private CachingFileIO newCachingFileIO(
            FileIO delegate, LocalCacheManager cache, EnumSet<FileType> whitelist, int blockSize) {
        return new CachingFileIO(delegate, cache, whitelist);
    }

    @Test
    void testShortRemoteReadIsNotCachedAsZeroPaddedBlock() throws IOException {
        byte[] data = "truncated".getBytes();
        MockFileIO delegate = new MockFileIO();
        // the status says 8 bytes more than the stream can hand out
        delegate.addTruncatedFile("snapshot-1", data, data.length + 8);

        LocalDiskCacheManager cache = new LocalDiskCacheManager(cacheDir, Long.MAX_VALUE, 64);
        CachingFileIO cachingIO = newCachingFileIO(delegate, cache, EnumSet.of(FileType.META), 64);

        try (SeekableInputStream s = cachingIO.newInputStream(new Path("snapshot-1"))) {
            assertThatThrownBy(() -> readAll(s, data.length + 8))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Premature EOF");
        }
    }

    @Test
    void testMetaFileIsCached() throws IOException {
        byte[] data = "snapshot data".getBytes();
        MockFileIO delegate = new MockFileIO();
        delegate.addFile("snapshot-1", data);

        LocalDiskCacheManager cache = new LocalDiskCacheManager(cacheDir, Long.MAX_VALUE, 64);
        CachingFileIO cachingIO =
                newCachingFileIO(
                        delegate, cache, EnumSet.of(FileType.META, FileType.GLOBAL_INDEX), 64);

        // first read
        try (SeekableInputStream s = cachingIO.newInputStream(new Path("snapshot-1"))) {
            byte[] result = readAll(s, data.length);
            assertThat(result).isEqualTo(data);
        }

        // second read should still work (cache hit)
        try (SeekableInputStream s = cachingIO.newInputStream(new Path("snapshot-1"))) {
            byte[] result = readAll(s, data.length);
            assertThat(result).isEqualTo(data);
        }

        assertThat(delegate.getFileStatusCallCount("snapshot-1")).isEqualTo(2);
    }

    @Test
    void testManifestFileIsCached() throws IOException {
        byte[] data = "manifest data".getBytes();
        MockFileIO delegate = new MockFileIO();
        delegate.addFile("manifest-abc", data);

        LocalDiskCacheManager cache = new LocalDiskCacheManager(cacheDir, Long.MAX_VALUE, 64);
        CachingFileIO cachingIO =
                newCachingFileIO(
                        delegate, cache, EnumSet.of(FileType.META, FileType.GLOBAL_INDEX), 64);

        try (SeekableInputStream s = cachingIO.newInputStream(new Path("manifest-abc"))) {
            assertThat(readAll(s, data.length)).isEqualTo(data);
        }
    }

    @Test
    void testGlobalIndexFileIsCached() throws IOException {
        byte[] data = "index data".getBytes();
        MockFileIO delegate = new MockFileIO();
        delegate.addFile("global-index-uuid.index", data);

        LocalDiskCacheManager cache = new LocalDiskCacheManager(cacheDir, Long.MAX_VALUE, 64);
        CachingFileIO cachingIO =
                newCachingFileIO(
                        delegate, cache, EnumSet.of(FileType.META, FileType.GLOBAL_INDEX), 64);

        try (SeekableInputStream s =
                cachingIO.newInputStream(new Path("global-index-uuid.index"))) {
            assertThat(readAll(s, data.length)).isEqualTo(data);
        }
    }

    @Test
    void testCacheIsRecreatedAfterSerialization() throws Exception {
        byte[] data = "index data".getBytes();
        MockFileIO delegate = new MockFileIO();
        delegate.addFile("global-index-uuid.index", data);

        CatalogContext context = localCacheContext();
        CachingFileIO cachingIO = newCatalogCachingFileIO(delegate, context);

        CachingFileIO restored = InstantiationUtil.clone(cachingIO);

        try (SeekableInputStream stream =
                restored.newInputStream(new Path("global-index-uuid.index"))) {
            assertThat(stream).isInstanceOf(CachingSeekableInputStream.class);
            assertThat(readAll(stream, data.length)).isEqualTo(data);
        }
        restored.close();
    }

    @Test
    void testDeserializedCopiesOfSameWrapperShareCacheInSameJvm() throws Exception {
        byte[] data = "shared index data".getBytes();
        String fileName = "global-index-shared-uuid.index";

        CatalogContext context = localCacheContext();

        MockFileIO delegate = new MockFileIO();
        delegate.addFile(fileName, data);
        CachingFileIO original = newCatalogCachingFileIO(delegate, context);
        CachingFileIO first = InstantiationUtil.clone(original);
        CachingFileIO second = InstantiationUtil.clone(original);

        try (SeekableInputStream stream = first.newInputStream(new Path(fileName))) {
            assertThat(readAll(stream, data.length)).isEqualTo(data);
        }
        assertThat(MockFileIO.globalInputStreamCallCount(fileName)).isEqualTo(1);

        try (SeekableInputStream stream = second.newInputStream(new Path(fileName))) {
            assertThat(readAll(stream, data.length)).isEqualTo(data);
        }
        assertThat(MockFileIO.globalInputStreamCallCount(fileName)).isEqualTo(1);

        first.close();
        second.close();
    }

    @Test
    void testDeserializedWrappersFromDifferentManagersDoNotShareCache() throws Exception {
        byte[] firstData = "first catalog data".getBytes();
        byte[] secondData = "other catalog data".getBytes();
        String fileName = "global-index-isolated-uuid.index";

        CatalogContext context = localCacheContext();

        MockFileIO firstDelegate = new MockFileIO();
        firstDelegate.addFile(fileName, firstData);
        CachingFileIO first =
                InstantiationUtil.clone(newCatalogCachingFileIO(firstDelegate, context));

        MockFileIO secondDelegate = new MockFileIO();
        secondDelegate.addFile(fileName, secondData);
        CachingFileIO second =
                InstantiationUtil.clone(newCatalogCachingFileIO(secondDelegate, context));

        try (SeekableInputStream stream = first.newInputStream(new Path(fileName))) {
            assertThat(readAll(stream, firstData.length)).isEqualTo(firstData);
        }

        try (SeekableInputStream stream = second.newInputStream(new Path(fileName))) {
            assertThat(readAll(stream, secondData.length)).isEqualTo(secondData);
        }
        assertThat(MockFileIO.globalInputStreamCallCount(fileName)).isEqualTo(2);

        first.close();
        second.close();
    }

    @Test
    void testWrappersWithSameManagerUseDifferentSecurityScopes() throws Exception {
        byte[] firstData = "first security data".getBytes();
        byte[] secondData = "other security data".getBytes();
        String fileName = "global-index-security-scope-uuid.index";
        CatalogContext context = localCacheContext();
        LocalCacheManager cache = CachingFileIO.createCacheManager(context);

        MockFileIO firstDelegate = new MockFileIO();
        firstDelegate.addFile(fileName, firstData);
        CachingFileIO first =
                InstantiationUtil.clone(
                        (CachingFileIO)
                                CachingFileIO.wrapWithCachingIfNeeded(
                                        firstDelegate, context, cache));

        MockFileIO secondDelegate = new MockFileIO();
        secondDelegate.addFile(fileName, secondData);
        CachingFileIO second =
                InstantiationUtil.clone(
                        (CachingFileIO)
                                CachingFileIO.wrapWithCachingIfNeeded(
                                        secondDelegate, context, cache));

        try (SeekableInputStream stream = first.newInputStream(new Path(fileName))) {
            assertThat(readAll(stream, firstData.length)).isEqualTo(firstData);
        }
        try (SeekableInputStream stream = second.newInputStream(new Path(fileName))) {
            assertThat(readAll(stream, secondData.length)).isEqualTo(secondData);
        }

        first.close();
        second.close();
    }

    @Test
    void testDeserializedWrappersShareConfiguredMemoryLimit() throws Exception {
        byte[] firstData = "first-cache-block".getBytes();
        byte[] secondData = "second-cache-data".getBytes();
        String firstFile = "global-index-limit-first.index";
        String secondFile = "global-index-limit-second.index";
        CatalogContext context = localCacheContext(firstData.length);

        MockFileIO firstDelegate = new MockFileIO();
        firstDelegate.addFile(firstFile, firstData);
        CachingFileIO first =
                InstantiationUtil.clone(newCatalogCachingFileIO(firstDelegate, context));

        MockFileIO secondDelegate = new MockFileIO();
        secondDelegate.addFile(secondFile, secondData);
        CachingFileIO second =
                InstantiationUtil.clone(newCatalogCachingFileIO(secondDelegate, context));

        try (SeekableInputStream stream = first.newInputStream(new Path(firstFile))) {
            assertThat(readAll(stream, firstData.length)).isEqualTo(firstData);
        }
        try (SeekableInputStream stream = second.newInputStream(new Path(secondFile))) {
            assertThat(readAll(stream, secondData.length)).isEqualTo(secondData);
        }
        try (SeekableInputStream stream = first.newInputStream(new Path(firstFile))) {
            assertThat(readAll(stream, firstData.length)).isEqualTo(firstData);
        }

        assertThat(MockFileIO.globalInputStreamCallCount(firstFile)).isEqualTo(2);
        first.close();
        second.close();
    }

    @Test
    void testDeserializedCacheIsReleasedAfterAllExistingWrappersClose() throws Exception {
        byte[] data = "released cache data".getBytes();
        String fileName = "global-index-released-uuid.index";

        MockFileIO delegate = new MockFileIO();
        delegate.addFile(fileName, data);
        CachingFileIO original = newCatalogCachingFileIO(delegate, localCacheContext());

        CachingFileIO first = InstantiationUtil.clone(original);
        CachingFileIO second = InstantiationUtil.clone(original);
        try (SeekableInputStream stream = first.newInputStream(new Path(fileName))) {
            assertThat(readAll(stream, data.length)).isEqualTo(data);
        }
        first.close();

        try (SeekableInputStream stream = second.newInputStream(new Path(fileName))) {
            assertThat(readAll(stream, data.length)).isEqualTo(data);
        }
        assertThat(MockFileIO.globalInputStreamCallCount(fileName)).isEqualTo(1);
        second.close();

        CachingFileIO third = InstantiationUtil.clone(original);
        try (SeekableInputStream stream = third.newInputStream(new Path(fileName))) {
            assertThat(readAll(stream, data.length)).isEqualTo(data);
        }
        assertThat(MockFileIO.globalInputStreamCallCount(fileName)).isEqualTo(2);
        third.close();
    }

    @Test
    void testDiskCacheIsReusedAfterManagerRecreation() throws Exception {
        byte[] data = "persistent disk cache data".getBytes();
        String fileName = "global-index-persistent-uuid.index";
        CatalogContext context = localDiskCacheContext();

        MockFileIO firstDelegate = new MockFileIO();
        firstDelegate.addFile(fileName, data);
        CachingFileIO first = newCatalogCachingFileIO(firstDelegate, context);
        try (SeekableInputStream stream = first.newInputStream(new Path(fileName))) {
            assertThat(readAll(stream, data.length)).isEqualTo(data);
        }
        first.close();

        MockFileIO secondDelegate = new MockFileIO();
        secondDelegate.addFile(fileName, data);
        CachingFileIO second = newCatalogCachingFileIO(secondDelegate, context);
        try (SeekableInputStream stream = second.newInputStream(new Path(fileName))) {
            assertThat(readAll(stream, data.length)).isEqualTo(data);
        }
        assertThat(MockFileIO.globalInputStreamCallCount(fileName)).isEqualTo(1);
        second.close();
    }

    private CatalogContext localCacheContext() {
        return localCacheContext(null);
    }

    private CatalogContext localCacheContext(@Nullable Integer maxSize) {
        Options options = new Options();
        options.set(LOCAL_CACHE_ENABLED, true);
        options.set(LOCAL_CACHE_WHITELIST, "global-index");
        if (maxSize != null) {
            options.set(LOCAL_CACHE_MAX_SIZE, MemorySize.ofBytes(maxSize));
        }
        return CatalogContext.create(options);
    }

    private CatalogContext localDiskCacheContext() {
        Options options = new Options();
        options.set(LOCAL_CACHE_ENABLED, true);
        options.set(LOCAL_CACHE_DIR, cacheDir);
        options.set(LOCAL_CACHE_WHITELIST, "global-index");
        return CatalogContext.create(options);
    }

    private CachingFileIO newCatalogCachingFileIO(FileIO delegate, CatalogContext context) {
        return (CachingFileIO)
                CachingFileIO.wrapWithCachingIfNeeded(
                        delegate, context, CachingFileIO.createCacheManager(context));
    }

    @Test
    void testDataFileNotCached() throws IOException {
        byte[] data = "data content".getBytes();
        MockFileIO delegate = new MockFileIO();
        delegate.addFile("data-abc.orc", data);

        LocalDiskCacheManager cache = new LocalDiskCacheManager(cacheDir, Long.MAX_VALUE, 64);
        CachingFileIO cachingIO =
                newCachingFileIO(
                        delegate, cache, EnumSet.of(FileType.META, FileType.GLOBAL_INDEX), 64);

        SeekableInputStream s = cachingIO.newInputStream(new Path("data-abc.orc"));
        assertThat(s).isNotInstanceOf(CachingSeekableInputStream.class);
        byte[] result = readAll(s, data.length);
        assertThat(result).isEqualTo(data);
        s.close();
        // getFileStatus should NOT be called for data files
        assertThat(delegate.getFileStatusCallCount("data-abc.orc")).isEqualTo(0);
    }

    @Test
    void testFileIndexNotCached() throws IOException {
        byte[] data = "file index content".getBytes();
        MockFileIO delegate = new MockFileIO();
        delegate.addFile("data-abc.orc.index", data);

        LocalDiskCacheManager cache = new LocalDiskCacheManager(cacheDir, Long.MAX_VALUE, 64);
        CachingFileIO cachingIO =
                newCachingFileIO(
                        delegate, cache, EnumSet.of(FileType.META, FileType.GLOBAL_INDEX), 64);

        SeekableInputStream s = cachingIO.newInputStream(new Path("data-abc.orc.index"));
        assertThat(s).isNotInstanceOf(CachingSeekableInputStream.class);
        s.close();
    }

    @Test
    void testCustomWhitelistMetaOnly() throws IOException {
        MockFileIO delegate = new MockFileIO();
        delegate.addFile("snapshot-1", "snap".getBytes());
        delegate.addFile("global-index-uuid.index", "idx".getBytes());

        LocalDiskCacheManager cache = new LocalDiskCacheManager(cacheDir, Long.MAX_VALUE, 64);
        CachingFileIO cachingIO = newCachingFileIO(delegate, cache, EnumSet.of(FileType.META), 64);

        SeekableInputStream s1 = cachingIO.newInputStream(new Path("snapshot-1"));
        assertThat(s1).isInstanceOf(CachingSeekableInputStream.class);
        s1.close();

        SeekableInputStream s2 = cachingIO.newInputStream(new Path("global-index-uuid.index"));
        assertThat(s2).isNotInstanceOf(CachingSeekableInputStream.class);
        s2.close();
    }

    @Test
    void testBucketIndexCachedWhenInWhitelist() throws IOException {
        byte[] data = "bucket index".getBytes();
        MockFileIO delegate = new MockFileIO();
        delegate.addFile("index-uuid-0", data);

        LocalDiskCacheManager cache = new LocalDiskCacheManager(cacheDir, Long.MAX_VALUE, 64);
        CachingFileIO cachingIO =
                newCachingFileIO(
                        delegate,
                        cache,
                        EnumSet.of(FileType.META, FileType.GLOBAL_INDEX, FileType.BUCKET_INDEX),
                        64);

        try (SeekableInputStream s = cachingIO.newInputStream(new Path("index-uuid-0"))) {
            assertThat(s).isInstanceOf(CachingSeekableInputStream.class);
            assertThat(readAll(s, data.length)).isEqualTo(data);
        }
    }

    @Test
    void testCacheHitAvoidsRemoteRead() throws IOException {
        byte[] data = "0123456789abcdef".getBytes();
        MockFileIO delegate = new MockFileIO();
        delegate.addFile("snapshot-1", data);

        LocalDiskCacheManager cache = new LocalDiskCacheManager(cacheDir, Long.MAX_VALUE, 8);
        CachingFileIO cachingIO =
                newCachingFileIO(
                        delegate, cache, EnumSet.of(FileType.META, FileType.GLOBAL_INDEX), 64);

        // first read populates cache
        try (SeekableInputStream s = cachingIO.newInputStream(new Path("snapshot-1"))) {
            readAll(s, data.length);
        }
        int firstReadCount = delegate.newInputStreamCallCount("snapshot-1");
        assertThat(firstReadCount).isEqualTo(1);

        // second read should hit cache — delegate.newInputStream should NOT be called
        // because the remote stream is lazily opened and all blocks are cached
        try (SeekableInputStream s = cachingIO.newInputStream(new Path("snapshot-1"))) {
            byte[] result = readAll(s, data.length);
            assertThat(result).isEqualTo(data);
        }
        assertThat(delegate.newInputStreamCallCount("snapshot-1")).isEqualTo(firstReadCount);
    }

    @Test
    void testMutableFilesNotCached() throws IOException {
        MockFileIO delegate = new MockFileIO();
        delegate.addFile("EARLIEST", "1".getBytes());
        delegate.addFile("LATEST", "42".getBytes());

        LocalDiskCacheManager cache = new LocalDiskCacheManager(cacheDir, Long.MAX_VALUE, 64);
        CachingFileIO cachingIO =
                newCachingFileIO(
                        delegate, cache, EnumSet.of(FileType.META, FileType.GLOBAL_INDEX), 64);

        // EARLIEST and LATEST are META but mutable — should not be cached
        SeekableInputStream s1 = cachingIO.newInputStream(new Path("EARLIEST"));
        assertThat(s1).isNotInstanceOf(CachingSeekableInputStream.class);
        s1.close();

        SeekableInputStream s2 = cachingIO.newInputStream(new Path("LATEST"));
        assertThat(s2).isNotInstanceOf(CachingSeekableInputStream.class);
        s2.close();

        assertThat(delegate.getFileStatusCallCount("EARLIEST")).isEqualTo(0);
        assertThat(delegate.getFileStatusCallCount("LATEST")).isEqualTo(0);
    }

    @Test
    void testReadSpanningMultipleBlocks() throws IOException {
        byte[] data = new byte[1024];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }
        MockFileIO delegate = new MockFileIO();
        delegate.addFile("snapshot-1", data);

        LocalDiskCacheManager cache = new LocalDiskCacheManager(cacheDir, Long.MAX_VALUE, 100);
        CachingFileIO cachingIO =
                newCachingFileIO(
                        delegate, cache, EnumSet.of(FileType.META, FileType.GLOBAL_INDEX), 64);

        try (SeekableInputStream s = cachingIO.newInputStream(new Path("snapshot-1"))) {
            // read across block boundary: block 0 ends at 100, block 1 starts at 100
            s.seek(90);
            byte[] result = new byte[30];
            int read = readFully(s, result);
            assertThat(read).isEqualTo(30);
            byte[] expected = new byte[30];
            System.arraycopy(data, 90, expected, 0, 30);
            assertThat(result).isEqualTo(expected);
        }
    }

    @Test
    void testSeekAndRead() throws IOException {
        byte[] data = "0123456789abcdef".getBytes();
        MockFileIO delegate = new MockFileIO();
        delegate.addFile("snapshot-1", data);

        LocalDiskCacheManager cache = new LocalDiskCacheManager(cacheDir, Long.MAX_VALUE, 8);
        CachingFileIO cachingIO =
                newCachingFileIO(
                        delegate, cache, EnumSet.of(FileType.META, FileType.GLOBAL_INDEX), 64);

        try (SeekableInputStream s = cachingIO.newInputStream(new Path("snapshot-1"))) {
            s.seek(10);
            byte[] result = new byte[6];
            readFully(s, result);
            assertThat(new String(result)).isEqualTo("abcdef");
        }
    }

    @Test
    void testDelegateMethodsForwarded() throws IOException {
        MockFileIO delegate = new MockFileIO();
        delegate.addFile("snapshot-1", "data".getBytes());

        LocalDiskCacheManager cache = new LocalDiskCacheManager(cacheDir, Long.MAX_VALUE, 64);
        CachingFileIO cachingIO =
                newCachingFileIO(
                        delegate, cache, EnumSet.of(FileType.META, FileType.GLOBAL_INDEX), 64);

        assertThat(cachingIO.exists(new Path("snapshot-1"))).isTrue();
        assertThat(cachingIO.exists(new Path("nonexistent"))).isFalse();
        assertThat(cachingIO.isObjectStore()).isFalse();
    }

    @Test
    void testConcurrentFirstReadOpensSingleRemoteStream() throws Exception {
        byte[] data = positionMarkedBytes(256);
        MockFileIO delegate = new MockFileIO();
        delegate.addFile("global-index-concurrent.index", data);
        CountDownLatch openGate = new CountDownLatch(1);
        delegate.blockOpensUntil(openGate);

        CachingSeekableInputStream stream =
                new CachingSeekableInputStream(
                        delegate,
                        new Path("global-index-concurrent.index"),
                        new LocalMemoryCacheManager(Long.MAX_VALUE, 64),
                        "concurrent",
                        data.length);

        AtomicReference<Throwable> failure = new AtomicReference<>();
        byte[] firstBlock = new byte[64];
        byte[] secondBlock = new byte[64];
        Thread first = readerThread("first", stream, 0, firstBlock, failure);
        Thread second = readerThread("second", stream, 64, secondBlock, failure);

        try {
            // parks inside delegate.newInputStream, holding the lazy initialisation lock
            first.start();
            assertThat(awaitCondition(() -> delegate.openCount() == 1, 30_000)).isTrue();

            // a different block, so this reader has to go remote as well
            second.start();

            // guarded, second queues on the lock; unguarded, it opens a stream of its own. The
            // BLOCKED sample is only a fast exit for the passing case - a guard built on something
            // other than an intrinsic lock would simply wait out the budget and still pass.
            awaitCondition(
                    () -> second.getState() == Thread.State.BLOCKED || delegate.openCount() == 2,
                    5_000);
            assertThat(delegate.openCount()).as("remote streams opened").isEqualTo(1);
        } finally {
            openGate.countDown();
        }

        first.join(30_000);
        second.join(30_000);
        assertThat(first.isAlive()).isFalse();
        assertThat(second.isAlive()).isFalse();
        assertThat(failure.get()).isNull();

        assertThat(firstBlock).isEqualTo(Arrays.copyOfRange(data, 0, 64));
        assertThat(secondBlock).isEqualTo(Arrays.copyOfRange(data, 64, 128));

        stream.close();
        assertThat(delegate.unclosedStreamCount()).as("remote streams left open").isZero();
        assertThat(delegate.openCount()).as("remote streams opened").isEqualTo(1);
    }

    @Test
    void testReadAfterCloseDoesNotReopenRemoteStream() throws Exception {
        byte[] data = positionMarkedBytes(256);
        MockFileIO delegate = new MockFileIO();
        delegate.addFile("global-index-after-close.index", data);

        CachingSeekableInputStream stream =
                new CachingSeekableInputStream(
                        delegate,
                        new Path("global-index-after-close.index"),
                        new LocalMemoryCacheManager(Long.MAX_VALUE, 64),
                        "after-close",
                        data.length);

        byte[] firstBlock = new byte[64];
        stream.preadFully(0, firstBlock, 0, firstBlock.length);
        assertThat(delegate.openCount()).isEqualTo(1);

        stream.close();
        assertThat(delegate.unclosedStreamCount()).as("remote streams left open").isZero();

        // a read that still needs the remote must fail rather than open a stream with no owner
        byte[] secondBlock = new byte[64];
        assertThatThrownBy(() -> stream.preadFully(64, secondBlock, 0, secondBlock.length))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Stream is closed");
        assertThat(delegate.openCount()).as("remote streams opened").isEqualTo(1);
        assertThat(delegate.unclosedStreamCount()).as("remote streams left open").isZero();

        // and block 0 is still cached, so without a guard on the read itself this would quietly
        // succeed and post-close behaviour would depend on whether a block happened to be resident
        assertThatThrownBy(() -> stream.preadFully(0, new byte[64], 0, 64))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Stream is closed");
        stream.seek(0);
        assertThatThrownBy(() -> stream.read(new byte[64], 0, 64))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Stream is closed");
        assertThatThrownBy(stream::read)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Stream is closed");
    }

    @Test
    void testCloseDoesNotWaitForAnInFlightOpenAndLeavesNothingBehind() throws Exception {
        byte[] data = positionMarkedBytes(256);
        MockFileIO delegate = new MockFileIO();
        delegate.addFile("global-index-close-race.index", data);
        CountDownLatch openGate = new CountDownLatch(1);
        delegate.blockOpensUntil(openGate);

        CachingSeekableInputStream stream =
                new CachingSeekableInputStream(
                        delegate,
                        new Path("global-index-close-race.index"),
                        new LocalMemoryCacheManager(Long.MAX_VALUE, 64),
                        "close-race",
                        data.length);

        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread reader = readerThread("reader", stream, 0, new byte[64], failure);
        Thread closer = new Thread(() -> closeRecordingFailure(stream, failure), "closer");
        closer.setDaemon(true);

        try {
            reader.start();
            assertThat(awaitCondition(() -> delegate.openCount() == 1, 30_000)).isTrue();

            // the reader is parked inside newInputStream, so a close that took the initialisation
            // lock would sit here until the remote gave up. This half does not regress against the
            // old code, which took no lock either - it guards against the obvious wrong fix.
            closer.start();
            closer.join(60_000);
            assertThat(closer.isAlive())
                    .as("close() blocked behind an in-flight remote open")
                    .isFalse();
        } finally {
            openGate.countDown();
        }

        // close is already gone, so the thread that opened the stream has to hand it back itself
        reader.join(30_000);
        assertThat(reader.isAlive()).isFalse();
        assertThat(failure.get())
                .as("the reader that lost the race should be told the stream is closed")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Stream is closed");
        assertThat(delegate.openCount()).as("remote streams opened").isEqualTo(1);
        assertThat(delegate.unclosedStreamCount()).as("remote streams left open").isZero();
    }

    @Test
    void testFailedOpenLeavesNoStateBehind() throws Exception {
        byte[] data = positionMarkedBytes(256);
        MockFileIO delegate = new MockFileIO();
        delegate.addFile("global-index-failed-open.index", data);
        delegate.failOpensWith(new IOException("boom"));

        CachingSeekableInputStream stream =
                new CachingSeekableInputStream(
                        delegate,
                        new Path("global-index-failed-open.index"),
                        new LocalMemoryCacheManager(Long.MAX_VALUE, 64),
                        "failed-open",
                        data.length);

        byte[] block = new byte[64];
        assertThatThrownBy(() -> stream.preadFully(0, block, 0, block.length))
                .isInstanceOf(IOException.class)
                .hasMessage("boom");
        assertThat(delegate.openCount()).isZero();

        // the failure must not have poisoned the stream: a retry opens exactly once
        delegate.failOpensWith(null);
        stream.preadFully(0, block, 0, block.length);
        assertThat(block).isEqualTo(Arrays.copyOfRange(data, 0, 64));
        assertThat(delegate.openCount()).as("remote streams opened").isEqualTo(1);

        stream.close();
        stream.close();
        assertThat(delegate.unclosedStreamCount()).as("remote streams left open").isZero();
    }

    @Test
    void testVectoredReadOpensSingleRemoteStream() throws Exception {
        byte[] data = positionMarkedBytes(VECTOR_STRIDE * VECTOR_RANGES);
        MockFileIO delegate = new MockFileIO();
        delegate.addFile("global-index-vectored.index", data);
        CountDownLatch openGate = new CountDownLatch(1);
        delegate.blockOpensUntil(openGate);

        // the file size is resolved lazily here, exercising the lazy path that only
        // the testing constructor still uses
        CachingSeekableInputStream stream =
                new CachingSeekableInputStream(
                        delegate,
                        new Path("global-index-vectored.index"),
                        new LocalMemoryCacheManager(Long.MAX_VALUE, 512));

        List<FileRange> ranges = vectorRanges();
        try {
            VectoredReadUtils.readVectored(stream, ranges, vectorReadOptions(stream));
            // with the first opener parked, every other task piles into the lazy init. A guarded
            // init can never reach two, so waiting out the full budget here is the passing case.
            assertThat(awaitCondition(() -> delegate.openCount() >= 2, 2_000))
                    .as("a second remote stream was opened while the first open was in flight")
                    .isFalse();
        } finally {
            openGate.countDown();
        }

        for (int i = 0; i < VECTOR_RANGES; i++) {
            int offset = i * VECTOR_STRIDE;
            assertThat(ranges.get(i).getData().get(60, TimeUnit.SECONDS))
                    .isEqualTo(Arrays.copyOfRange(data, offset, offset + VECTOR_LENGTH));
        }

        stream.close();
        assertThat(delegate.unclosedStreamCount()).as("remote streams left open").isZero();
        assertThat(delegate.openCount()).as("remote streams opened").isEqualTo(1);
    }

    @Test
    void testCloseDuringVectoredFanOutLeavesNothingBehind() throws Exception {
        byte[] data = positionMarkedBytes(VECTOR_STRIDE * VECTOR_RANGES);

        // the production shutdown shape: the owner closes while pool tasks are still reading
        for (int attempt = 0; attempt < 20; attempt++) {
            MockFileIO delegate = new MockFileIO();
            delegate.addFile("global-index-close-fanout.index", data);
            CachingSeekableInputStream stream =
                    new CachingSeekableInputStream(
                            delegate,
                            new Path("global-index-close-fanout.index"),
                            new LocalMemoryCacheManager(Long.MAX_VALUE, 512),
                            "close-fanout-" + attempt,
                            data.length);

            List<FileRange> ranges = vectorRanges();
            VectoredReadUtils.readVectored(stream, ranges, vectorReadOptions(stream));
            stream.close();

            for (int i = 0; i < VECTOR_RANGES; i++) {
                int offset = i * VECTOR_STRIDE;
                try {
                    // a range that made it through must still carry its own bytes, not another's
                    assertThat(ranges.get(i).getData().get(60, TimeUnit.SECONDS))
                            .isEqualTo(Arrays.copyOfRange(data, offset, offset + VECTOR_LENGTH));
                } catch (ExecutionException e) {
                    // losing the race against close is legitimate, but only for that reason
                    assertThat(e.getCause())
                            .as("attempt %d, range %d", attempt, i)
                            .isInstanceOf(IOException.class)
                            .hasMessageContaining("closed");
                }
            }

            // whichever way the interleaving fell, the accounting has to come out even
            assertThat(delegate.openCount())
                    .as("attempt %d: remote streams opened", attempt)
                    .isLessThanOrEqualTo(1);
            assertThat(delegate.unclosedStreamCount())
                    .as("attempt %d: remote streams left open", attempt)
                    .isZero();
        }
    }

    private static List<FileRange> vectorRanges() {
        List<FileRange> ranges = new ArrayList<>();
        for (int i = 0; i < VECTOR_RANGES; i++) {
            ranges.add(FileRange.createFileRange((long) i * VECTOR_STRIDE, VECTOR_LENGTH));
        }
        return ranges;
    }

    /** The options NativeVectorGlobalIndexReader uses for global index files. */
    private static VectoredReadUtils.ReadOptions vectorReadOptions(
            CachingSeekableInputStream stream) {
        // the stride clears the minimum seek, so the ranges stay unmerged and each becomes its own
        // task; parallelism must stay >= VECTOR_RANGES or readVectored blocks the calling thread
        return VectoredReadUtils.ReadOptions.from(stream)
                .withMinSeekForVectorReads(16 * 1024)
                .withParallelismForVectorReads(VECTOR_PARALLELISM)
                .withSequentialReadFallback(false);
    }

    private static void closeRecordingFailure(
            CachingSeekableInputStream stream, AtomicReference<Throwable> failure) {
        try {
            stream.close();
        } catch (Throwable t) {
            failure.compareAndSet(null, t);
        }
    }

    private static Thread readerThread(
            String name,
            CachingSeekableInputStream stream,
            long position,
            byte[] buffer,
            AtomicReference<Throwable> failure) {
        Thread thread =
                new Thread(
                        () -> {
                            try {
                                stream.preadFully(position, buffer, 0, buffer.length);
                            } catch (Throwable t) {
                                failure.compareAndSet(null, t);
                            }
                        },
                        name);
        thread.setDaemon(true);
        return thread;
    }

    /** Returns whether the condition became true before the timeout elapsed. */
    private static boolean awaitCondition(BooleanSupplier condition, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() - deadline >= 0) {
                return false;
            }
            Thread.sleep(1);
        }
        return true;
    }

    /**
     * Position-dependent filler. A plain {@code (byte) i} repeats every 256 bytes, so every vector
     * range would hold identical content and a range served another range's bytes would go
     * unnoticed - exactly the corruption a shared remote stream can produce.
     */
    private static byte[] positionMarkedBytes(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) (i ^ (i >> 5) ^ (i >> 11));
        }
        return data;
    }

    private byte[] readAll(SeekableInputStream s, int size) throws IOException {
        byte[] buf = new byte[size];
        int off = 0;
        while (off < size) {
            int n = s.read(buf, off, size - off);
            if (n < 0) {
                break;
            }
            off += n;
        }
        return buf;
    }

    private int readFully(SeekableInputStream s, byte[] buf) throws IOException {
        int off = 0;
        while (off < buf.length) {
            int n = s.read(buf, off, buf.length - off);
            if (n < 0) {
                break;
            }
            off += n;
        }
        return off;
    }

    /** Simple in-memory FileIO for testing. */
    private static class MockFileIO implements FileIO {

        private static final Map<String, AtomicInteger> GLOBAL_INPUT_STREAM_CALLS =
                new ConcurrentHashMap<>();

        private final Map<String, byte[]> files = new HashMap<>();
        private final Map<String, Long> reportedLengths = new HashMap<>();
        private final Map<String, Long> mtimes = new HashMap<>();
        // concurrent so the thread-safety tests below can count from several reader threads
        private final Map<String, Integer> fileStatusCalls = new ConcurrentHashMap<>();
        private final Map<String, Integer> newInputStreamCalls = new ConcurrentHashMap<>();
        private final List<ByteArraySeekableInputStream> openedStreams =
                new CopyOnWriteArrayList<>();

        private final AtomicInteger openCount = new AtomicInteger();

        @Nullable private volatile CountDownLatch openGate;
        @Nullable private volatile IOException openFailure;

        static void resetGlobalInputStreamCalls() {
            GLOBAL_INPUT_STREAM_CALLS.clear();
        }

        /** Parks every open inside {@link #newInputStream} until the latch is counted down. */
        void blockOpensUntil(CountDownLatch gate) {
            this.openGate = gate;
        }

        /** Makes the next opens fail, until cleared with {@code null}. */
        void failOpensWith(@Nullable IOException failure) {
            this.openFailure = failure;
        }

        int openCount() {
            return openCount.get();
        }

        int unclosedStreamCount() {
            int unclosed = 0;
            for (ByteArraySeekableInputStream stream : openedStreams) {
                if (!stream.isClosed()) {
                    unclosed++;
                }
            }
            return unclosed;
        }

        static int globalInputStreamCallCount(String name) {
            AtomicInteger count = GLOBAL_INPUT_STREAM_CALLS.get(name);
            return count == null ? 0 : count.get();
        }

        void addFile(String name, byte[] data, long mtime) {
            files.put(name, data);
            mtimes.put(name, mtime);
        }

        void addFile(String name, byte[] data) {
            files.put(name, data);
        }

        /** Reports a length beyond the bytes on hand, the way a truncated remote file does. */
        void addTruncatedFile(String name, byte[] data, long reportedLength) {
            files.put(name, data);
            reportedLengths.put(name, reportedLength);
        }

        int getFileStatusCallCount(String name) {
            return fileStatusCalls.getOrDefault(name, 0);
        }

        int newInputStreamCallCount(String name) {
            return newInputStreamCalls.getOrDefault(name, 0);
        }

        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            String name = path.getName();
            // rejected before any counter moves, so all of them agree on what was handed out
            IOException failure = openFailure;
            if (failure != null) {
                throw failure;
            }
            newInputStreamCalls.merge(name, 1, Integer::sum);
            GLOBAL_INPUT_STREAM_CALLS
                    .computeIfAbsent(name, ignored -> new AtomicInteger())
                    .incrementAndGet();
            byte[] data = files.get(name);
            if (data == null) {
                throw new IOException("File not found: " + name);
            }
            // registered before parking, so openCount() never over-reports what is tracked
            ByteArraySeekableInputStream stream = new ByteArraySeekableInputStream(data);
            openedStreams.add(stream);
            openCount.incrementAndGet();
            CountDownLatch gate = openGate;
            if (gate != null) {
                try {
                    // far beyond every budget gated behind the countdown, so a slow machine can
                    // never release the gate early and turn a correct run red
                    if (!gate.await(5, TimeUnit.MINUTES)) {
                        throw new IOException("open gate was never released");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException(e);
                }
            }
            return stream;
        }

        @Override
        public PositionOutputStream newOutputStream(Path path, boolean overwrite) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileStatus getFileStatus(Path path) throws IOException {
            String name = path.getName();
            fileStatusCalls.merge(name, 1, Integer::sum);
            byte[] data = files.get(name);
            if (data == null) {
                throw new IOException("File not found: " + name);
            }
            return new FileStatus() {
                @Override
                public long getLen() {
                    return reportedLengths.getOrDefault(name, (long) data.length);
                }

                @Override
                public boolean isDir() {
                    return false;
                }

                @Override
                public Path getPath() {
                    return path;
                }

                @Override
                public long getModificationTime() {
                    return mtimes.getOrDefault(name, 0L);
                }
            };
        }

        @Override
        public FileStatus[] listStatus(Path path) {
            return new FileStatus[0];
        }

        @Override
        public boolean exists(Path path) {
            return files.containsKey(path.getName());
        }

        @Override
        public boolean delete(Path path, boolean recursive) {
            return files.remove(path.getName()) != null;
        }

        @Override
        public boolean mkdirs(Path path) {
            return true;
        }

        @Override
        public boolean rename(Path src, Path dst) {
            return false;
        }

        @Override
        public boolean isObjectStore() {
            return false;
        }

        @Override
        public void configure(CatalogContext context) {}
    }

    /** SeekableInputStream backed by a byte array, recording whether it was closed. */
    private static class ByteArraySeekableInputStream extends SeekableInputStream {

        private final byte[] data;
        private final AtomicBoolean closed = new AtomicBoolean();

        private int pos;

        ByteArraySeekableInputStream(byte[] data) {
            this.data = data;
            this.pos = 0;
        }

        boolean isClosed() {
            return closed.get();
        }

        // a real remote stream rejects reads once closed, so this one has to as well: otherwise no
        // test could ever observe a read racing a close
        private void checkNotClosed() throws IOException {
            if (closed.get()) {
                throw new IOException("Stream is closed");
            }
        }

        @Override
        public void seek(long desired) throws IOException {
            checkNotClosed();
            this.pos = (int) Math.max(0, Math.min(desired, data.length));
        }

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public int read() throws IOException {
            checkNotClosed();
            if (pos >= data.length) {
                return -1;
            }
            return data[pos++] & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            checkNotClosed();
            if (pos >= data.length) {
                return -1;
            }
            int toRead = Math.min(len, data.length - pos);
            System.arraycopy(data, pos, b, off, toRead);
            pos += toRead;
            return toRead;
        }

        @Override
        public void close() {
            closed.set(true);
        }
    }
}
