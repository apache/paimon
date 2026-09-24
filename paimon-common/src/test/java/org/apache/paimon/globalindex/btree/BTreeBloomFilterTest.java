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

import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.KeySerializer;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.sst.BloomFilterHandle;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.utils.BloomFilter;
import org.apache.paimon.utils.MurmurHashUtils;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the optional Bloom filter in BTree index files. */
class BTreeBloomFilterTest {

    private static final int ENTRY_COUNT = 1_000;

    @Test
    void testBloomFormatCompatibleWithPython() {
        assertThat(MurmurHashUtils.hashBytes("a".getBytes())).isEqualTo(1485273170);
        assertThat(MurmurHashUtils.hashBytes("hello".getBytes())).isEqualTo(-1008564952);
        assertThat(MurmurHashUtils.hashBytes("world".getBytes())).isEqualTo(-623458850);

        BloomFilter.Builder builder = BloomFilter.fixedBuilder(3, 0.05);
        builder.addHash(MurmurHashUtils.hashBytes("a".getBytes()));
        builder.addHash(MurmurHashUtils.hashBytes("hello".getBytes()));
        builder.addHash(MurmurHashUtils.hashBytes("world".getBytes()));

        assertThat(builder.build().getMemorySegment().getArray())
                .isEqualTo(new byte[] {(byte) 0xea, 0x22, (byte) 0x99});
    }

    @Test
    void testBloomFilterIsNotWrittenByDefault() throws Exception {
        Fixture fixture = writeIndex(new Options());

        assertThat(fixture.footer.getBloomFilterHandle()).isNull();
    }

    @Test
    void testBloomFilterAvoidsDataReadForMissingLookups() throws Exception {
        Options options = new Options();
        options.set(BTreeIndexOptions.BTREE_INDEX_BLOOM_FILTER_ENABLED, true);
        Fixture fixture = writeIndex(options);
        BloomFilterHandle bloomHandle = fixture.footer.getBloomFilterHandle();

        assertThat(bloomHandle).isNotNull();
        assertThat(bloomHandle.expectedEntries()).isEqualTo(ENTRY_COUNT);

        int missingKey = findRejectedMissingKey(fixture.bytes, bloomHandle);
        assertMissingLookupReadsOnlyBloom(
                fixture, bloomHandle, reader -> reader.visitEqual(missingKey));
        assertMissingLookupReadsOnlyBloom(
                fixture,
                bloomHandle,
                reader -> reader.visitIn(Collections.singletonList(missingKey)));
    }

    private void assertMissingLookupReadsOnlyBloom(
            Fixture fixture, BloomFilterHandle bloomHandle, Lookup lookup) throws Exception {
        CountingInput input = new CountingInput(fixture.bytes);
        try (CacheManager cacheManager = new CacheManager(MemorySize.ofMebiBytes(1), 0.5);
                BTreeIndexReader reader =
                        new BTreeIndexReader(
                                KeySerializer.create(new IntType()),
                                ignored -> input,
                                fixture.meta,
                                cacheManager)) {
            input.resetCount();

            assertThat(lookup.apply(reader).get().results()).isEmpty();
            assertThat(input.bytesRead).isEqualTo(bloomHandle.size());
        }
    }

    private Fixture writeIndex(Options options) throws IOException {
        ByteArrayGlobalIndexFileWriter fileWriter = new ByteArrayGlobalIndexFileWriter();
        BTreeGlobalIndexer indexer =
                new BTreeGlobalIndexer(new DataField(0, "k", new IntType()), options);
        GlobalIndexSingleColumnWriter writer = indexer.createWriter(fileWriter);
        for (int i = 0; i < ENTRY_COUNT; i++) {
            writer.write(i * 2, i);
        }
        List<ResultEntry> entries = writer.finish();
        assertThat(entries).hasSize(1);

        byte[] bytes = fileWriter.bytes();
        ResultEntry entry = entries.get(0);
        BTreeFileFooter footer =
                BTreeFileFooter.readFooter(
                        MemorySlice.wrap(bytes)
                                .slice(
                                        bytes.length - BTreeFileFooter.ENCODED_LENGTH,
                                        BTreeFileFooter.ENCODED_LENGTH)
                                .toInput());
        GlobalIndexIOMeta meta =
                new GlobalIndexIOMeta(
                        new Path(entry.fileName()), bytes.length, entry.rowCount(), entry.meta());
        return new Fixture(bytes, footer, meta);
    }

    private int findRejectedMissingKey(byte[] bytes, BloomFilterHandle handle) {
        BloomFilter filter = new BloomFilter(handle.expectedEntries(), handle.size());
        filter.setMemorySegment(MemorySegment.wrap(bytes), (int) handle.offset());
        KeySerializer serializer = KeySerializer.create(new IntType());
        for (int candidate = 1; candidate < ENTRY_COUNT * 2; candidate += 2) {
            if (!filter.testHash(MurmurHashUtils.hashBytes(serializer.serialize(candidate)))) {
                return candidate;
            }
        }
        throw new AssertionError("Unable to find a Bloom-filter-negative missing key.");
    }

    private static class Fixture {
        private final byte[] bytes;
        private final BTreeFileFooter footer;
        private final GlobalIndexIOMeta meta;

        private Fixture(byte[] bytes, BTreeFileFooter footer, GlobalIndexIOMeta meta) {
            this.bytes = bytes;
            this.footer = footer;
            this.meta = meta;
        }
    }

    @FunctionalInterface
    private interface Lookup {
        Optional<GlobalIndexResult> apply(BTreeIndexReader reader);
    }

    private static class ByteArrayGlobalIndexFileWriter implements GlobalIndexFileWriter {
        private final ByteArrayPositionOutputStream output = new ByteArrayPositionOutputStream();

        @Override
        public String newFileName(String prefix) {
            return prefix + "-test";
        }

        @Override
        public PositionOutputStream newOutputStream(String fileName) {
            return output;
        }

        private byte[] bytes() {
            return output.bytes();
        }
    }

    private static class ByteArrayPositionOutputStream extends PositionOutputStream {
        private final ByteArrayOutputStream output = new ByteArrayOutputStream();

        @Override
        public long getPos() {
            return output.size();
        }

        @Override
        public void write(int value) {
            output.write(value);
        }

        @Override
        public void write(byte[] bytes) {
            output.write(bytes, 0, bytes.length);
        }

        @Override
        public void write(byte[] bytes, int offset, int length) {
            output.write(bytes, offset, length);
        }

        @Override
        public void flush() {}

        @Override
        public void close() {}

        private byte[] bytes() {
            return output.toByteArray();
        }
    }

    private static class CountingInput extends ByteArraySeekableStream {
        private int bytesRead;

        private CountingInput(byte[] bytes) {
            super(bytes);
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            int read = super.read(bytes, offset, length);
            bytesRead += Math.max(read, 0);
            return read;
        }

        private void resetCount() {
            bytesRead = 0;
        }
    }
}
