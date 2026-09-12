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

package org.apache.paimon.sst;

import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.compression.BlockCompressionType;
import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.io.cache.CacheKey;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.utils.BloomFilter;
import org.apache.paimon.utils.FileBasedBloomFilter;
import org.apache.paimon.utils.MurmurHashUtils;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the combined block/trailer cache without changing the SST format. */
class SstFileReaderCacheTest {

    @ParameterizedTest
    @CsvSource({"NONE,false", "NONE,true", "LZ4,false", "LZ4,true"})
    void testRejectedBloomUsesCachedData(String compression, boolean offHeap) throws Exception {
        BloomFixture fixture = new BloomFixture(compression, 16 * 1024);
        CountingInput input = new CountingInput(fixture.bytes);
        try (CacheManager manager =
                        offHeap
                                ? CacheManager.createOffHeap(MemorySize.ofMebiBytes(1), 0.5)
                                : new CacheManager(MemorySize.ofMebiBytes(1), 0.5);
                SstFileReader reader = fixture.reader(input, manager)) {
            assertThat(reader.lookup(new byte[] {2})).containsExactly(fixture.value);
            assertThat(manager.contains(fixture.bloomKey())).isFalse();
            assertThat(manager.dataCache().asMap()).hasSize(1);

            input.reads = 0;
            assertThat(reader.lookup(new byte[] {4})).containsExactly(fixture.value);
            assertThat(reader.lookup(new byte[] {1})).isNull();
            assertThat(input.reads).isZero();

            // Another reader can admit the filter later. Use it without loading the data block.
            manager.invalidPage(fixture.dataKey());
            manager.getPage(
                    fixture.bloomKey(),
                    ignored ->
                            Arrays.copyOfRange(
                                    fixture.bytes,
                                    (int) fixture.bloom.offset(),
                                    (int) fixture.bloom.offset() + fixture.bloom.size()));
            assertThat(reader.lookup(new byte[] {1})).isNull();
            assertThat(input.reads).isZero();
            assertThat(manager.dataCache().asMap()).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRejectedBloomCanRecoverInTheSameReader(boolean offHeap) throws Exception {
        BloomFixture fixture = new BloomFixture("NONE", 16 * 1024);
        CountingInput input = new CountingInput(fixture.bytes);
        try (CacheManager manager =
                        offHeap
                                ? CacheManager.createOffHeap(MemorySize.ofMebiBytes(1), 0.5)
                                : new CacheManager(MemorySize.ofMebiBytes(1), 0.5);
                SstFileReader reader = fixture.reader(input, manager)) {
            assertThat(reader.lookup(new byte[] {2})).containsExactly(fixture.value);
            assertThat(manager.contains(fixture.bloomKey())).isFalse();
            fixture.rejectBloom = false;
            input.bytesRead = 0;
            for (int i = 0; i < 64 && !manager.contains(fixture.bloomKey()); i++) {
                manager.invalidPage(fixture.dataKey());
                assertThat(reader.lookup(new byte[] {2})).containsExactly(fixture.value);
            }
            assertThat(manager.contains(fixture.bloomKey())).isTrue();
            assertThat(input.bytesRead)
                    .isLessThanOrEqualTo(2 * fixture.bloom.size() + 2 * fixture.dataSize);
            manager.invalidPage(fixture.dataKey());
            input.reads = 0;
            assertThat(reader.lookup(new byte[] {1})).isNull();
            assertThat(input.reads).isZero();
        }
    }

    @ParameterizedTest
    @CsvSource({
        "NONE,false,16384",
        "NONE,true,16384",
        "NONE,false,256",
        "NONE,true,256",
        "LZ4,false,16384",
        "LZ4,true,16384"
    })
    void testRejectedBloomReadCost(String compression, boolean offHeap, int bloomSize)
            throws Exception {
        BloomFixture fixture = new BloomFixture(compression, bloomSize);
        CountingInput input = new CountingInput(fixture.bytes);
        try (CacheManager manager =
                        offHeap
                                ? CacheManager.createOffHeap(MemorySize.ofMebiBytes(1), 0.5)
                                : new CacheManager(MemorySize.ofMebiBytes(1), 0.5);
                SstFileReader reader = fixture.reader(input, manager)) {
            assertThat(reader.lookup(new byte[] {2})).containsExactly(fixture.value);
            manager.invalidPage(fixture.dataKey());
            input.reads = 0;
            input.bytesRead = 0;
            assertThat(reader.lookup(new byte[] {1})).isNull();
            assertThat(input.reads).isOne();
            boolean cheaperBloom = bloomSize < fixture.dataSize || "LZ4".equals(compression);
            assertThat(input.bytesRead).isEqualTo(cheaperBloom ? bloomSize : fixture.dataSize);

            input.reads = 0;
            assertThat(reader.lookup(new byte[] {127})).isNull();
            assertThat(input.reads).isZero();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCompressedIndexKeepsUncacheableBloomOnReopen(boolean offHeap) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        PositionOutputStream out = output(bytes);
        SstFileWriter writer =
                new SstFileWriter(
                        out, 256, null, BlockCompressionFactory.create(BlockCompressionType.LZ4));
        byte[] value = new byte[64];
        for (int i = 0; i < 1000; i++) {
            MemorySegment key = MemorySegment.allocateHeapMemory(32);
            key.putInt(0, 2 * i);
            writer.put(key.getHeapMemory(), value);
        }
        writer.flush();
        BloomFilter bloom = new BloomFilter(1000, 128 * 1024);
        bloom.setMemorySegment(MemorySegment.allocateHeapMemory(128 * 1024), 0);
        for (int i = 0; i < 1000; i++) {
            MemorySegment key = MemorySegment.allocateHeapMemory(32);
            key.putInt(0, 2 * i);
            bloom.addHash(MurmurHashUtils.hashBytes(key.getHeapMemory()));
        }
        BloomFilterHandle bloomHandle = bloom.write(out);
        BlockHandle index = writer.writeIndexBlock();
        byte[] file = bytes.toByteArray();
        assertThat(file[(int) index.offset() + index.size()]).isNotZero();
        MemorySegment missing = MemorySegment.allocateHeapMemory(32);
        missing.putInt(0, 1);
        assertThat(bloom.testHash(MurmurHashUtils.hashBytes(missing.getHeapMemory()))).isFalse();
        Path path = new Path("compressed-index-bloom");
        try (CacheManager manager =
                offHeap
                        ? CacheManager.createOffHeap(MemorySize.ofKibiBytes(64), 0.5)
                        : new CacheManager(MemorySize.ofKibiBytes(64), 0.5)) {
            // Keep both readers open so the second borrows the already decoded index page.
            CountingInput first = new CountingInput(file);
            CountingInput second = new CountingInput(file);
            try (SstFileReader reader =
                            new SstFileReader(
                                    Comparator.comparingInt(slice -> slice.readInt(0)),
                                    new BlockCache(path, first, manager),
                                    index,
                                    FileBasedBloomFilter.create(
                                            first, path, manager, bloomHandle));
                    SstFileReader reopened =
                            new SstFileReader(
                                    Comparator.comparingInt(slice -> slice.readInt(0)),
                                    new BlockCache(path, second, manager),
                                    index,
                                    FileBasedBloomFilter.create(
                                            second, path, manager, bloomHandle))) {
                assertThat(first.reads).isOne();
                assertThat(second.reads).isZero();
                first.reads = 0;
                first.bytesRead = 0;
                assertThat(reader.lookup(missing.getHeapMemory())).isNull();
                assertThat(reopened.lookup(missing.getHeapMemory())).isNull();
                assertThat(first.reads).isOne();
                assertThat(second.reads).isOne();
                assertThat(first.bytesRead).isEqualTo(bloomHandle.size());
                assertThat(second.bytesRead).isEqualTo(bloomHandle.size());
                assertThat(manager.dataCache().asMap()).isEmpty();
            }
        }
    }

    @ParameterizedTest
    @CsvSource({
        "NONE,false,-1,0",
        "NONE,false,0,0",
        "NONE,false,1,0",
        "NONE,true,1,0",
        "LZ4,false,1,0",
        "LZ4,true,1,0",
        "LZ4,false,4096,0",
        "LZ4,true,4096,0",
        "NONE,false,1,1",
        "NONE,true,1,1",
        "NONE,false,1,2"
    })
    void testUncacheableBloomReadCost(
            String compression, boolean offHeap, int sizeDelta, int cacheBudget) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        PositionOutputStream out = output(bytes);
        SstFileWriter writer =
                new SstFileWriter(
                        out,
                        64 * 1024,
                        null,
                        BlockCompressionFactory.create(BlockCompressionType.valueOf(compression)));
        byte[] value = new byte[64];
        Arrays.fill(value, (byte) 42);
        for (int i = 0; i < 16; i++) {
            writer.put(new byte[] {(byte) (2 * i)}, value);
        }
        writer.flush();
        int dataBlockSize = bytes.size();
        BloomFilter bloom = new BloomFilter(1000, dataBlockSize + sizeDelta);
        bloom.setMemorySegment(MemorySegment.allocateHeapMemory(dataBlockSize + sizeDelta), 0);
        for (int i = 0; i < 16; i++) {
            bloom.addHash(MurmurHashUtils.hashBytes(new byte[] {(byte) (2 * i)}));
        }
        byte[] missing = new byte[] {1};
        while (missing[0] < 31 && bloom.testHash(MurmurHashUtils.hashBytes(missing))) {
            missing[0] += 2;
        }
        assertThat(bloom.testHash(MurmurHashUtils.hashBytes(missing))).isFalse();
        BloomFilterHandle bloomHandle = bloom.write(out);
        BlockHandle index = writer.writeIndexBlock();
        CountingInput input = new CountingInput(bytes.toByteArray());
        Path path = new Path("bloom-read-cost");
        boolean cached = cacheBudget == 1;
        // Budget 2 admits the data block but cannot fit the larger Bloom in the index pool.
        MemorySize capacity =
                cached
                        ? MemorySize.ofMebiBytes(1)
                        : MemorySize.ofBytes(cacheBudget == 2 ? 2L * (bloomHandle.size() - 1) : 0);
        try (CacheManager manager =
                        offHeap
                                ? CacheManager.createOffHeap(capacity, 0.5)
                                : new CacheManager(capacity, 0.5);
                SstFileReader reader =
                        new SstFileReader(
                                Comparator.comparingInt(slice -> slice.readByte(0)),
                                new BlockCache(path, input, manager),
                                index,
                                FileBasedBloomFilter.create(input, path, manager, bloomHandle))) {
            input.reads = 0;
            input.bytesRead = 0;
            assertThat(reader.lookup(new byte[] {2})).containsExactly(value);
            boolean probeBloom = cached || sizeDelta < 0;
            assertThat(input.reads).isEqualTo(probeBloom ? 2 : 1);
            assertThat(input.bytesRead)
                    .isEqualTo(dataBlockSize + (probeBloom ? bloomHandle.size() : 0));

            // Force the absent-key lookup to choose between reading Bloom and reading data.
            manager.invalidPage(CacheKey.forPosition(path, 0, dataBlockSize, false));
            input.reads = 0;
            input.bytesRead = 0;
            assertThat(reader.lookup(missing)).isNull();
            assertThat(input.reads).isEqualTo(cached ? 0 : 1);
            // A compressed block can be much larger to decode than its serialized size.
            boolean probeOnMissing = probeBloom || "LZ4".equals(compression);
            assertThat(input.bytesRead)
                    .isEqualTo(cached ? 0 : probeOnMissing ? bloomHandle.size() : dataBlockSize);
            input.reads = 0;
            assertThat(reader.lookup(new byte[] {127})).isNull();
            assertThat(input.reads).isZero();
            if (cacheBudget == 0) {
                assertThat(manager.dataCache().asMap()).isEmpty();
                assertThat(manager.indexCache().asMap()).isEmpty();
            } else if (cacheBudget == 2) {
                assertThat(manager.dataCache().asMap()).hasSize(1);
                assertThat(
                                manager.contains(
                                        CacheKey.forPosition(
                                                path,
                                                bloomHandle.offset(),
                                                bloomHandle.size(),
                                                true)))
                        .isFalse();
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"NONE,false", "NONE,true", "LZ4,false", "LZ4,true"})
    void testOneReadAndCacheEntryPerBlock(String compression, boolean offHeap) throws Exception {
        Fixture fixture = new Fixture(compression);
        CountingInput input = new CountingInput(fixture.bytes);
        try (CacheManager manager =
                        offHeap
                                ? CacheManager.createOffHeap(MemorySize.ofMebiBytes(1), 0.5)
                                : new CacheManager(MemorySize.ofMebiBytes(1), 0.5);
                SstFileReader reader = fixture.reader(input, manager)) {
            assertThat(input.reads).isOne();
            assertThat(reader.lookup(new byte[] {2})).containsExactly(fixture.value);
            assertThat(input.reads).isEqualTo(2);
            assertThat(manager.dataCache().asMap()).hasSize(1);
            assertThat(manager.indexCache().asMap()).hasSize(1);
            assertThat(reader.lookup(new byte[] {3})).containsExactly(fixture.value);
            assertThat(input.reads).isEqualTo(2);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"NONE", "LZ4"})
    void testCorruptBlockIsRejectedAndCanBeRetried(String compression) throws Exception {
        Fixture fixture = new Fixture(compression);
        CountingInput input = new CountingInput(fixture.bytes);
        try (CacheManager manager = new CacheManager(MemorySize.ofMebiBytes(1), 0.5);
                SstFileReader reader = fixture.reader(input, manager)) {
            fixture.bytes[0] ^= 1;
            assertThatThrownBy(() -> reader.lookup(new byte[] {2}))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Expected CRC32C");
            assertThat(manager.dataCache().asMap()).isEmpty();
            fixture.bytes[0] ^= 1;
            assertThat(reader.lookup(new byte[] {2})).containsExactly(fixture.value);
            assertThat(manager.dataCache().asMap()).hasSize(1);
        }
    }

    private static class BloomFixture {
        private final Path path = new Path("rejected-bloom");
        private final byte[] value = new byte[64];
        private final byte[] bytes;
        private final int dataSize;
        private final BlockHandle index;
        private final BloomFilterHandle bloom;
        private boolean rejectBloom = true;

        private BloomFixture(String compression, int bloomSize) throws IOException {
            Arrays.fill(value, (byte) 42);
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            PositionOutputStream out = output(bytes);
            SstFileWriter writer =
                    new SstFileWriter(
                            out,
                            64 * 1024,
                            null,
                            BlockCompressionFactory.create(
                                    BlockCompressionType.valueOf(compression)));
            BloomFilter filter = new BloomFilter(16, bloomSize);
            filter.setMemorySegment(MemorySegment.allocateHeapMemory(bloomSize), 0);
            for (int i = 0; i < 16; i++) {
                byte[] key = new byte[] {(byte) (2 * i)};
                writer.put(key, value);
                filter.addHash(MurmurHashUtils.hashBytes(key));
            }
            writer.flush();
            this.dataSize = bytes.size();
            this.bloom = filter.write(out);
            this.index = writer.writeIndexBlock();
            this.bytes = bytes.toByteArray();
            assertThat(filter.testHash(MurmurHashUtils.hashBytes(new byte[] {1}))).isFalse();
            assertThat(this.bytes[dataSize - BlockTrailer.ENCODED_LENGTH] == 0)
                    .isEqualTo("NONE".equals(compression));
        }

        private CacheKey bloomKey() {
            return CacheKey.forPosition(path, bloom.offset(), bloom.size(), true);
        }

        private CacheKey dataKey() {
            return CacheKey.forPosition(path, 0, dataSize, false);
        }

        private SstFileReader reader(CountingInput input, CacheManager manager) {
            FileBasedBloomFilter filter =
                    new FileBasedBloomFilter(
                            input, path, manager, 16, bloom.offset(), bloom.size()) {
                        @Override
                        public boolean testHash(int hash) {
                            boolean result = super.testHash(hash);
                            // Admission is optional even when the page fits. Force this legal
                            // outcome without depending on Caffeine's frequency sketch or timing.
                            if (rejectBloom) {
                                manager.invalidPage(bloomKey());
                            }
                            return result;
                        }
                    };
            assertThat(filter.isCacheable()).isTrue();
            return new SstFileReader(
                    Comparator.comparingInt(slice -> slice.readByte(0)),
                    new BlockCache(path, input, manager),
                    index,
                    filter);
        }
    }

    private static class Fixture {
        private final byte[] value = new byte[2048];
        private final byte[] bytes;
        private final BlockHandle index;

        private Fixture(String compression) throws IOException {
            Arrays.fill(value, (byte) 42);
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            PositionOutputStream out = output(bytes);
            SstFileWriter writer =
                    new SstFileWriter(
                            out,
                            64 * 1024,
                            null,
                            BlockCompressionFactory.create(
                                    BlockCompressionType.valueOf(compression)));
            for (int i = 1; i <= 3; i++) {
                writer.put(new byte[] {(byte) i}, value);
            }
            writer.flush();
            this.index = writer.writeIndexBlock();
            this.bytes = bytes.toByteArray();
            // Verify the fixture really exercises compressed data, not the writer's raw fallback.
            int compressionId = this.bytes[(int) index.offset() - BlockTrailer.ENCODED_LENGTH];
            assertThat(compressionId == 0).isEqualTo("NONE".equals(compression));
        }

        private SstFileReader reader(CountingInput input, CacheManager manager) {
            return new SstFileReader(
                    Comparator.comparingInt(slice -> slice.readByte(0)),
                    new BlockCache(new Path("sst-file"), input, manager),
                    index,
                    null);
        }
    }

    private static PositionOutputStream output(ByteArrayOutputStream bytes) {
        return new PositionOutputStream() {
            @Override
            public void close() {}

            @Override
            public void flush() {}

            @Override
            public long getPos() {
                return bytes.size();
            }

            @Override
            public void write(int b) {
                bytes.write(b);
            }

            @Override
            public void write(byte[] b) {
                bytes.write(b, 0, b.length);
            }

            @Override
            public void write(byte[] b, int offset, int length) {
                bytes.write(b, offset, length);
            }
        };
    }

    private static class CountingInput extends ByteArraySeekableStream {
        private int reads;
        private int bytesRead;

        private CountingInput(byte[] bytes) {
            super(bytes);
        }

        @Override
        public int read(byte[] b, int offset, int length) throws IOException {
            reads++;
            int read = super.read(b, offset, length);
            bytesRead += Math.max(read, 0);
            return read;
        }
    }
}
