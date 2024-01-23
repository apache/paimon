/*
 * Copyright 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

/* This file is based on source code of StorageReader from the PalDB Project (https://github.com/linkedin/PalDB), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

package org.apache.paimon.lookup.hash;

import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.io.cache.CachedRandomInputView;
import org.apache.paimon.lookup.LookupStoreReader;
import org.apache.paimon.utils.MurmurHashUtils;
import org.apache.paimon.utils.VarLengthIntUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Map;

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
    private CachedRandomInputView inputView;
    // Buffers
    private final byte[] slotBuffer;

    HashLookupStoreReader(CacheManager cacheManager, int cachePageSize, File file)
            throws IOException {
        // File path
        if (!file.exists()) {
            throw new FileNotFoundException("File " + file.getAbsolutePath() + " not found");
        }
        LOG.info("Opening file {}", file.getName());

        // Open file and read metadata
        long createdAt;
        FileInputStream inputStream = new FileInputStream(file);
        DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(inputStream));
        // Offset of the index in the channel
        int keyCount;
        int indexOffset;
        long dataOffset;
        try {
            // Time
            createdAt = dataInputStream.readLong();

            // Metadata counters
            keyCount = dataInputStream.readInt();
            // Number of different key length
            int keyLengthCount = dataInputStream.readInt();
            // Max key length
            int maxKeyLength = dataInputStream.readInt();

            // Read offset counts and keys
            indexOffsets = new int[maxKeyLength + 1];
            dataOffsets = new long[maxKeyLength + 1];
            keyCounts = new int[maxKeyLength + 1];
            slots = new int[maxKeyLength + 1];
            slotSizes = new int[maxKeyLength + 1];

            int maxSlotSize = 0;
            for (int i = 0; i < keyLengthCount; i++) {
                int keyLength = dataInputStream.readInt();

                keyCounts[keyLength] = dataInputStream.readInt();
                slots[keyLength] = dataInputStream.readInt();
                slotSizes[keyLength] = dataInputStream.readInt();
                indexOffsets[keyLength] = dataInputStream.readInt();
                dataOffsets[keyLength] = dataInputStream.readLong();

                maxSlotSize = Math.max(maxSlotSize, slotSizes[keyLength]);
            }

            slotBuffer = new byte[maxSlotSize];

            // Read index offset to resign indexOffsets
            indexOffset = dataInputStream.readInt();
            for (int i = 0; i < indexOffsets.length; i++) {
                indexOffsets[i] = indexOffset + indexOffsets[i];
            }
            // Read data offset to resign dataOffsets
            dataOffset = dataInputStream.readLong();
            for (int i = 0; i < dataOffsets.length; i++) {
                dataOffsets[i] = dataOffset + dataOffsets[i];
            }
        } finally {
            // Close metadata
            dataInputStream.close();
            inputStream.close();
        }

        // Create Mapped file in read-only mode
        inputView = new CachedRandomInputView(file, cacheManager, cachePageSize);

        // logging
        DecimalFormat integerFormat = new DecimalFormat("#,##0.00");
        StringBuilder statMsg = new StringBuilder("Storage metadata\n");
        statMsg.append("  Created at: ").append(formatCreatedAt(createdAt)).append("\n");
        statMsg.append("  Key count: ").append(keyCount).append("\n");
        for (int i = 0; i < keyCounts.length; i++) {
            if (keyCounts[i] > 0) {
                statMsg.append("  Key count for key length ")
                        .append(i)
                        .append(": ")
                        .append(keyCounts[i])
                        .append("\n");
            }
        }
        statMsg.append("  Index size: ")
                .append(integerFormat.format((dataOffset - indexOffset) / (1024.0 * 1024.0)))
                .append(" Mb\n");
        statMsg.append("  Data size: ")
                .append(integerFormat.format((file.length() - dataOffset) / (1024.0 * 1024.0)))
                .append(" Mb\n");
        LOG.info(statMsg.toString());
    }

    @Override
    public byte[] lookup(byte[] key) throws IOException {
        int keyLength = key.length;
        if (keyLength >= slots.length || keyCounts[keyLength] == 0) {
            return null;
        }
        long hash = MurmurHashUtils.hashBytesPositive(key);
        int numSlots = slots[keyLength];
        int slotSize = slotSizes[keyLength];
        int indexOffset = indexOffsets[keyLength];
        long dataOffset = dataOffsets[keyLength];

        for (int probe = 0; probe < numSlots; probe++) {
            long slot = (hash + probe) % numSlots;
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

    private String formatCreatedAt(long createdAt) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd G 'at' HH:mm:ss z");
        Calendar cl = Calendar.getInstance();
        cl.setTimeInMillis(createdAt);
        return sdf.format(cl.getTime());
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
