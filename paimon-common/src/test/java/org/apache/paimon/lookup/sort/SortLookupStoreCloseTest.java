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

package org.apache.paimon.lookup.sort;

import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.cache.CacheKey;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.memory.MemorySliceOutput;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.utils.BloomFilter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Closing a lookup store walks a three-level chain: {@link SortLookupStoreReader#close()} closes
 * the {@code SstFileReader} and then the file handle; the reader closes the bloom filter and then
 * the {@code BlockCache}; and the block cache hands every cached page back to the shared {@link
 * CacheManager}. Each level used to be a sequence of bare calls, so a single failing page
 * invalidation abandoned everything after it — including the file descriptor two levels up.
 *
 * <p>That descriptor is never reclaimed: both callers of {@code close()} in {@code LocalKvDb}
 * deliberately catch and log so one bad reader cannot stall shutdown, so the leak surfaces only as
 * a warning line.
 */
class SortLookupStoreCloseTest {

    private static final int BLOCK_SIZE = 10 * 256;

    @TempDir java.nio.file.Path tempPath;

    private FileIO fileIO;
    private Path file;
    private File localFile;

    @BeforeEach
    void beforeEach() throws Exception {
        this.fileIO = LocalFileIO.create();
        this.file = new Path(new Path(tempPath.toUri()), UUID.randomUUID().toString());
        this.localFile = new File(file.toUri().getPath());
        writeData(500);
    }

    @Test
    void closeReleasesTheFileHandleWhenPageInvalidationFails() throws Exception {
        ThrowingCacheManager cacheManager = new ThrowingCacheManager(true);
        TrackingInputStream input =
                new TrackingInputStream(LocalFileIO.INSTANCE.newInputStream(file));

        SortLookupStoreReader reader = newReader(input, cacheManager);
        // Populate the block cache so close() has pages to hand back.
        lookup(reader, 0);

        Throwable thrown = catchThrowable(reader::close);

        assertThat(thrown).isInstanceOf(RuntimeException.class).hasMessage("invalidPage failed");
        // The bloom filter is closed first and fails on its own page. The block cache must still
        // be closed afterwards, so more than that single page is handed back.
        assertThat(cacheManager.invalidated.get()).isGreaterThan(1);
        // And the descriptor is released even though every level below it failed.
        assertThat(input.closed).isTrue();
    }

    @Test
    void blockCacheHandsBackEveryPageWhenOneInvalidationFails() throws Exception {
        ThrowingCacheManager cacheManager = new ThrowingCacheManager(false);
        TrackingInputStream input =
                new TrackingInputStream(LocalFileIO.INSTANCE.newInputStream(file));

        SortLookupStoreReader reader = newReader(input, cacheManager);
        // Several lookups across the key range so more than one block is cached.
        for (int key = 0; key < 400; key += 40) {
            lookup(reader, key * 2);
        }
        int cachedPages = cacheManager.pagesTaken.get();
        assertThat(cachedPages).isGreaterThan(1);

        cacheManager.failFrom(1);
        catchThrowable(reader::close);

        // Every page was still offered back, not just the ones before the failure.
        assertThat(cacheManager.invalidated.get()).isEqualTo(cachedPages);
        assertThat(input.closed).isTrue();
    }

    @Test
    void closeIsSilentWhenNothingFails() throws Exception {
        CacheManager cacheManager = new CacheManager(MemorySize.ofMebiBytes(10), 0);
        TrackingInputStream input =
                new TrackingInputStream(LocalFileIO.INSTANCE.newInputStream(file));

        SortLookupStoreReader reader = newReader(input, cacheManager);
        lookup(reader, 0);
        reader.close();

        assertThat(input.closed).isTrue();
    }

    private SortLookupStoreReader newReader(SeekableInputStream input, CacheManager cacheManager) {
        return new SortLookupStoreReader(
                Comparator.comparingInt(slice -> slice.readInt(0)),
                file,
                localFile.length(),
                input,
                cacheManager);
    }

    private static void lookup(SortLookupStoreReader reader, int key) throws IOException {
        MemorySliceOutput keyOut = new MemorySliceOutput(4);
        keyOut.writeInt(key);
        reader.lookup(keyOut.toSlice().getHeapMemory());
    }

    private void writeData(int recordCount) throws Exception {
        BloomFilter.Builder bloomFilterBuilder = BloomFilter.fixedBuilder(recordCount, 0.05);
        BlockCompressionFactory compressionFactory = null;
        try (PositionOutputStream outputStream = fileIO.newOutputStream(file, true);
                SortLookupStoreWriter writer =
                        new SortLookupStoreWriter(
                                outputStream, BLOCK_SIZE, bloomFilterBuilder, compressionFactory)) {
            MemorySliceOutput keyOut = new MemorySliceOutput(4);
            MemorySliceOutput valueOut = new MemorySliceOutput(4);
            for (int i = 0; i < recordCount; i++) {
                keyOut.reset();
                valueOut.reset();
                keyOut.writeInt(i * 2);
                valueOut.writeInt(i * 2);
                writer.put(keyOut.toSlice().getHeapMemory(), valueOut.toSlice().getHeapMemory());
            }
        }
    }

    /** A cache manager whose page invalidation can be made to fail. */
    private static class ThrowingCacheManager extends CacheManager {

        private final AtomicInteger pagesTaken = new AtomicInteger();
        private final AtomicInteger invalidated = new AtomicInteger();
        private volatile int failFromCall;

        ThrowingCacheManager(boolean failEverything) {
            super(MemorySize.ofMebiBytes(10), 0);
            this.failFromCall = failEverything ? 0 : Integer.MAX_VALUE;
        }

        void failFrom(int call) {
            this.failFromCall = call;
        }

        @Override
        public org.apache.paimon.memory.MemorySegment getPage(
                CacheKey key,
                org.apache.paimon.io.cache.CacheReader reader,
                org.apache.paimon.io.cache.CacheCallback callback) {
            pagesTaken.incrementAndGet();
            return super.getPage(key, reader, callback);
        }

        @Override
        public void invalidPage(CacheKey key) {
            int call = invalidated.getAndIncrement();
            super.invalidPage(key);
            if (call >= failFromCall) {
                throw new RuntimeException("invalidPage failed");
            }
        }
    }

    /** Records whether the file handle was actually released. */
    private static class TrackingInputStream extends SeekableInputStream {

        private final SeekableInputStream delegate;
        private boolean closed;

        TrackingInputStream(SeekableInputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public void seek(long desired) throws IOException {
            delegate.seek(desired);
        }

        @Override
        public long getPos() throws IOException {
            return delegate.getPos();
        }

        @Override
        public int read() throws IOException {
            return delegate.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return delegate.read(b, off, len);
        }

        @Override
        public void close() throws IOException {
            closed = true;
            delegate.close();
        }
    }
}
