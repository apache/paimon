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

package org.apache.paimon.lookup.hash;

import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.io.PageFileInput;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.io.cache.FileBasedRandomInputView;
import org.apache.paimon.lookup.LookupStoreReader;
import org.apache.paimon.utils.FileBasedBloomFilter;
import org.apache.paimon.utils.MurmurHashUtils;
import org.apache.paimon.utils.VarLengthIntUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/* This file is based on source code of StorageReader from the PalDB Project (https://github.com/linkedin/PalDB), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Internal read implementation for hash kv store. */
public class HashLookupStoreReader
        implements LookupStoreReader, Iterable<Map.Entry<byte[], byte[]>> {

    private static final Logger LOG =
            LoggerFactory.getLogger(HashLookupStoreReader.class.getName());

    // Key count for each key length
    private final int[] keyCounts;
    // Slot size for each key length
    private final int[] slotSizes;
    // Number of slots for each key length
    private final int[] slots;
    // Offset of the index for different key length
    private final int[] indexOffsets;
    // Offset of the data for different key length
    private final long[] dataOffsets;
    // File input view
    private FileBasedRandomInputView inputView;
    // Buffers
    private final byte[] slotBuffer;

    @Nullable private FileBasedBloomFilter bloomFilter;

    HashLookupStoreReader(
            File file,
            HashContext context,
            CacheManager cacheManager,
            int cachePageSize,
            @Nullable BlockCompressionFactory compressionFactory)
            throws IOException {
        // File path
        if (!file.exists()) {
            throw new FileNotFoundException("File " + file.getAbsolutePath() + " not found");
        }

        keyCounts = context.keyCounts;
        slots = context.slots;
        slotSizes = context.slotSizes;
        int maxSlotSize = 0;
        for (int slotSize : slotSizes) {
            maxSlotSize = Math.max(maxSlotSize, slotSize);
        }
        slotBuffer = new byte[maxSlotSize];
        indexOffsets = context.indexOffsets;
        dataOffsets = context.dataOffsets;

        LOG.info("Opening file {}", file.getName());

        PageFileInput fileInput =
                PageFileInput.create(
                        file,
                        cachePageSize,
                        compressionFactory,
                        context.uncompressBytes,
                        context.compressPages);
        inputView = new FileBasedRandomInputView(fileInput, cacheManager);

        if (context.bloomFilterEnabled) {
            bloomFilter =
                    new FileBasedBloomFilter(
                            fileInput,
                            cacheManager,
                            context.bloomFilterExpectedEntries,
                            0,
                            context.bloomFilterBytes);
        }
    }

    @Override
    public byte[] lookup(byte[] key) throws IOException {
        int keyLength = key.length;
        if (keyLength >= slots.length || keyCounts[keyLength] == 0) {
            return null;
        }

        int hashcode = MurmurHashUtils.hashBytes(key);
        if (bloomFilter != null && !bloomFilter.testHash(hashcode)) {
            return null;
        }

        long hashPositive = hashcode & 0x7fffffff;
        int numSlots = slots[keyLength];
        int slotSize = slotSizes[keyLength];
        int indexOffset = indexOffsets[keyLength];
        long dataOffset = dataOffsets[keyLength];

        for (int probe = 0; probe < numSlots; probe++) {
            long slot = (hashPositive + probe) % numSlots;
            inputView.setReadPosition(indexOffset + slot * slotSize);
            inputView.readFully(slotBuffer, 0, slotSize);

            long offset = VarLengthIntUtils.decodeLong(slotBuffer, keyLength);
            if (offset == 0) {
                return null;
            }
            if (isKey(slotBuffer, key)) {
                return getValue(dataOffset + offset);
            }
        }
        return null;
    }

    private boolean isKey(byte[] slotBuffer, byte[] key) {
        for (int i = 0; i < key.length; i++) {
            if (slotBuffer[i] != key[i]) {
                return false;
            }
        }
        return true;
    }

    private byte[] getValue(long offset) throws IOException {
        inputView.setReadPosition(offset);

        // Get size of data
        int size = VarLengthIntUtils.decodeInt(inputView);

        // Create output bytes
        byte[] res = new byte[size];
        inputView.readFully(res);
        return res;
    }

    @Override
    public void close() throws IOException {
        inputView.close();
        inputView = null;
    }

    @Override
    public Iterator<Map.Entry<byte[], byte[]>> iterator() {
        return new StorageIterator(true);
    }

    public Iterator<Map.Entry<byte[], byte[]>> keys() {
        return new StorageIterator(false);
    }

    private class StorageIterator implements Iterator<Map.Entry<byte[], byte[]>> {

        private final FastEntry entry = new FastEntry();
        private final boolean withValue;
        private int currentKeyLength = 0;
        private byte[] currentSlotBuffer;
        private long keyIndex;
        private long keyLimit;
        private long currentDataOffset;
        private int currentIndexOffset;

        public StorageIterator(boolean value) {
            withValue = value;
            nextKeyLength();
        }

        private void nextKeyLength() {
            for (int i = currentKeyLength + 1; i < keyCounts.length; i++) {
                long c = keyCounts[i];
                if (c > 0) {
                    currentKeyLength = i;
                    keyLimit += c;
                    currentSlotBuffer = new byte[slotSizes[i]];
                    currentIndexOffset = indexOffsets[i];
                    currentDataOffset = dataOffsets[i];
                    break;
                }
            }
        }

        @Override
        public boolean hasNext() {
            return keyIndex < keyLimit;
        }

        @Override
        public FastEntry next() {
            try {
                inputView.setReadPosition(currentIndexOffset);

                long offset = 0;
                while (offset == 0) {
                    inputView.readFully(currentSlotBuffer);
                    offset = VarLengthIntUtils.decodeLong(currentSlotBuffer, currentKeyLength);
                    currentIndexOffset += currentSlotBuffer.length;
                }

                byte[] key = Arrays.copyOf(currentSlotBuffer, currentKeyLength);
                byte[] value = null;

                if (withValue) {
                    long valueOffset = currentDataOffset + offset;
                    value = getValue(valueOffset);
                }

                entry.set(key, value);

                if (++keyIndex == keyLimit) {
                    nextKeyLength();
                }
                return entry;
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        private class FastEntry implements Map.Entry<byte[], byte[]> {

            private byte[] key;
            private byte[] val;

            protected void set(byte[] k, byte[] v) {
                this.key = k;
                this.val = v;
            }

            @Override
            public byte[] getKey() {
                return key;
            }

            @Override
            public byte[] getValue() {
                return val;
            }

            @Override
            public byte[] setValue(byte[] value) {
                throw new UnsupportedOperationException("Not supported.");
            }
        }
    }
}
