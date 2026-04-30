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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.utils.FileType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CachingFileIO} and {@link CachingSeekableInputStream}. */
class CachingFileIOTest {

    @TempDir java.nio.file.Path tempDir;

    private String cacheDir;

    @BeforeEach
    void setUp() {
        cacheDir = tempDir.resolve("cache").toString();
    }

    @Test
    void testMetaFileIsCached() throws IOException {
        byte[] data = "snapshot data".getBytes();
        MockFileIO delegate = new MockFileIO();
        delegate.addFile("snapshot-1", data);

        BlockDiskCache cache = new BlockDiskCache(cacheDir, Long.MAX_VALUE, 64);
        CachingFileIO cachingIO =
                new CachingFileIO(
                        delegate, cache, EnumSet.of(FileType.META, FileType.GLOBAL_INDEX));

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

        BlockDiskCache cache = new BlockDiskCache(cacheDir, Long.MAX_VALUE, 64);
        CachingFileIO cachingIO =
                new CachingFileIO(
                        delegate, cache, EnumSet.of(FileType.META, FileType.GLOBAL_INDEX));

        try (SeekableInputStream s = cachingIO.newInputStream(new Path("manifest-abc"))) {
            assertThat(readAll(s, data.length)).isEqualTo(data);
        }
    }

    @Test
    void testGlobalIndexFileIsCached() throws IOException {
        byte[] data = "index data".getBytes();
        MockFileIO delegate = new MockFileIO();
        delegate.addFile("global-index-uuid.index", data);

        BlockDiskCache cache = new BlockDiskCache(cacheDir, Long.MAX_VALUE, 64);
        CachingFileIO cachingIO =
                new CachingFileIO(
                        delegate, cache, EnumSet.of(FileType.META, FileType.GLOBAL_INDEX));

        try (SeekableInputStream s =
                cachingIO.newInputStream(new Path("global-index-uuid.index"))) {
            assertThat(readAll(s, data.length)).isEqualTo(data);
        }
    }

    @Test
    void testDataFileNotCached() throws IOException {
        byte[] data = "data content".getBytes();
        MockFileIO delegate = new MockFileIO();
        delegate.addFile("data-abc.orc", data);

        BlockDiskCache cache = new BlockDiskCache(cacheDir, Long.MAX_VALUE, 64);
        CachingFileIO cachingIO =
                new CachingFileIO(
                        delegate, cache, EnumSet.of(FileType.META, FileType.GLOBAL_INDEX));

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

        BlockDiskCache cache = new BlockDiskCache(cacheDir, Long.MAX_VALUE, 64);
        CachingFileIO cachingIO =
                new CachingFileIO(
                        delegate, cache, EnumSet.of(FileType.META, FileType.GLOBAL_INDEX));

        SeekableInputStream s = cachingIO.newInputStream(new Path("data-abc.orc.index"));
        assertThat(s).isNotInstanceOf(CachingSeekableInputStream.class);
        s.close();
    }

    @Test
    void testCustomWhitelistMetaOnly() throws IOException {
        MockFileIO delegate = new MockFileIO();
        delegate.addFile("snapshot-1", "snap".getBytes());
        delegate.addFile("global-index-uuid.index", "idx".getBytes());

        BlockDiskCache cache = new BlockDiskCache(cacheDir, Long.MAX_VALUE, 64);
        CachingFileIO cachingIO = new CachingFileIO(delegate, cache, EnumSet.of(FileType.META));

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

        BlockDiskCache cache = new BlockDiskCache(cacheDir, Long.MAX_VALUE, 64);
        CachingFileIO cachingIO =
                new CachingFileIO(
                        delegate,
                        cache,
                        EnumSet.of(FileType.META, FileType.GLOBAL_INDEX, FileType.BUCKET_INDEX));

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

        BlockDiskCache cache = new BlockDiskCache(cacheDir, Long.MAX_VALUE, 8);
        CachingFileIO cachingIO =
                new CachingFileIO(
                        delegate, cache, EnumSet.of(FileType.META, FileType.GLOBAL_INDEX));

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

        BlockDiskCache cache = new BlockDiskCache(cacheDir, Long.MAX_VALUE, 64);
        CachingFileIO cachingIO =
                new CachingFileIO(
                        delegate, cache, EnumSet.of(FileType.META, FileType.GLOBAL_INDEX));

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

        BlockDiskCache cache = new BlockDiskCache(cacheDir, Long.MAX_VALUE, 100);
        CachingFileIO cachingIO =
                new CachingFileIO(
                        delegate, cache, EnumSet.of(FileType.META, FileType.GLOBAL_INDEX));

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

        BlockDiskCache cache = new BlockDiskCache(cacheDir, Long.MAX_VALUE, 8);
        CachingFileIO cachingIO =
                new CachingFileIO(
                        delegate, cache, EnumSet.of(FileType.META, FileType.GLOBAL_INDEX));

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

        BlockDiskCache cache = new BlockDiskCache(cacheDir, Long.MAX_VALUE, 64);
        CachingFileIO cachingIO =
                new CachingFileIO(
                        delegate, cache, EnumSet.of(FileType.META, FileType.GLOBAL_INDEX));

        assertThat(cachingIO.exists(new Path("snapshot-1"))).isTrue();
        assertThat(cachingIO.exists(new Path("nonexistent"))).isFalse();
        assertThat(cachingIO.isObjectStore()).isFalse();
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

        private final Map<String, byte[]> files = new HashMap<>();
        private final Map<String, Integer> fileStatusCalls = new HashMap<>();
        private final Map<String, Integer> newInputStreamCalls = new HashMap<>();

        void addFile(String name, byte[] data) {
            files.put(name, data);
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
            newInputStreamCalls.merge(name, 1, Integer::sum);
            byte[] data = files.get(name);
            if (data == null) {
                throw new IOException("File not found: " + name);
            }
            return new ByteArraySeekableInputStream(data);
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
                    return data.length;
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
                    return 0;
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

    /** SeekableInputStream backed by a byte array. */
    private static class ByteArraySeekableInputStream extends SeekableInputStream {

        private final byte[] data;
        private int pos;

        ByteArraySeekableInputStream(byte[] data) {
            this.data = data;
            this.pos = 0;
        }

        @Override
        public void seek(long desired) {
            this.pos = (int) Math.max(0, Math.min(desired, data.length));
        }

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public int read() {
            if (pos >= data.length) {
                return -1;
            }
            return data[pos++] & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (pos >= data.length) {
                return -1;
            }
            int toRead = Math.min(len, data.length - pos);
            System.arraycopy(data, pos, b, off, toRead);
            pos += toRead;
            return toRead;
        }

        @Override
        public void close() {}
    }
}
