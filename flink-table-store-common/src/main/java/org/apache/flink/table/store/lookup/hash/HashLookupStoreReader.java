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

/* This file is based on source code from the PalDB Project (https://github.com/linkedin/PalDB), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

package org.apache.flink.table.store.lookup.hash;

import org.apache.flink.table.store.lookup.LookupStoreReader;
import org.apache.flink.table.store.utils.MurmurHashUtils;
import org.apache.flink.table.store.utils.SimpleReadBuffer;
import org.apache.flink.table.store.utils.VarLengthIntUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
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

    // Buffer segment size
    private final long mmpSegmentSize;
    // Number of keys in the index
    private final int keyCount;
    // Key count for each key length
    private final int[] keyCounts;
    // Slot size for each key length
    private final int[] slotSizes;
    // Number of slots for each key length
    private final int[] slots;
    // Offset of the index for different key length
    private final int[] indexOffsets;
    // Offset of the data in the channel
    private final long dataOffset;
    // Offset of the data for different key length
    private final long[] dataOffsets;
    // Data size
    private final long dataSize;
    // Index and data buffers
    private MappedByteBuffer indexBuffer;
    private MappedByteBuffer[] dataBuffers;
    // FileChannel
    private RandomAccessFile mappedFile;
    private FileChannel channel;
    // Use MMap for data?
    private final boolean mMapData;
    // Buffers
    private final SimpleReadBuffer sizeBuffer = new SimpleReadBuffer(new byte[5]);
    private final byte[] slotBuffer;

    HashLookupStoreReader(boolean useMmp, long mmpSegmentSize, File file) throws IOException {
        // File path
        if (!file.exists()) {
            throw new FileNotFoundException("File " + file.getAbsolutePath() + " not found");
        }
        LOG.info("Opening file {}", file.getName());

        this.mmpSegmentSize = mmpSegmentSize;
        // Check valid segmentSize
        if (this.mmpSegmentSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("The mmpSegmentSize can't be larger than 2GB");
        }

        // Open file and read metadata
        long createdAt;
        FileInputStream inputStream = new FileInputStream(file);
        DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(inputStream));
        // Offset of the index in the channel
        int indexOffset;
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

            // Read index and data offset
            indexOffset = dataInputStream.readInt();
            dataOffset = dataInputStream.readLong();
        } finally {
            // Close metadata
            dataInputStream.close();
            inputStream.close();
        }

        // Create Mapped file in read-only mode
        mappedFile = new RandomAccessFile(file, "r");
        channel = mappedFile.getChannel();
        long fileSize = file.length();

        // Create index buffer
        indexBuffer =
                channel.map(FileChannel.MapMode.READ_ONLY, indexOffset, dataOffset - indexOffset);

        // Create data buffers
        dataSize = fileSize - dataOffset;

        // Check if data size fits in memory map limit
        if (!useMmp) {
            // Use classical disk read
            mMapData = false;
            dataBuffers = null;
        } else {
            // Use Mmap
            mMapData = true;

            // Build data buffers
            int bufArraySize =
                    (int) (dataSize / this.mmpSegmentSize)
                            + ((dataSize % this.mmpSegmentSize != 0) ? 1 : 0);
            dataBuffers = new MappedByteBuffer[bufArraySize];
            int bufIdx = 0;
            for (long offset = 0; offset < dataSize; offset += this.mmpSegmentSize) {
                long remainingFileSize = dataSize - offset;
                long thisSegmentSize = Math.min(this.mmpSegmentSize, remainingFileSize);
                dataBuffers[bufIdx++] =
                        channel.map(
                                FileChannel.MapMode.READ_ONLY,
                                dataOffset + offset,
                                thisSegmentSize);
            }
        }

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
                .append(integerFormat.format((fileSize - dataOffset) / (1024.0 * 1024.0)))
                .append(" Mb\n");
        if (mMapData) {
            statMsg.append("  Number of memory mapped data buffers: ").append(dataBuffers.length);
        } else {
            statMsg.append("  Memory mapped data disabled, using disk");
        }
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
            int slot = (int) ((hash + probe) % numSlots);
            indexBuffer.position(indexOffset + slot * slotSize);
            indexBuffer.get(slotBuffer, 0, slotSize);

            long offset = VarLengthIntUtils.decodeLong(slotBuffer, keyLength);
            if (offset == 0) {
                return null;
            }
            if (isKey(slotBuffer, key)) {
                return mMapData
                        ? getMMapBytes(dataOffset + offset)
                        : getDiskBytes(dataOffset + offset);
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

    // Close the reader channel
    public void close() throws IOException {
        channel.close();
        mappedFile.close();
        indexBuffer = null;
        dataBuffers = null;
        mappedFile = null;
        channel = null;
        System.gc();
    }

    public int getKeyCount() {
        return keyCount;
    }

    // Read the data at the given offset, the data can be spread over multiple data buffers
    private byte[] getMMapBytes(long offset) {
        // Read the first 4 bytes to get the size of the data
        ByteBuffer buf = getDataBuffer(offset);
        int maxLen = (int) Math.min(5, dataSize - offset);

        int size;
        if (buf.remaining() >= maxLen) {
            // Continuous read
            int pos = buf.position();
            size = VarLengthIntUtils.decodeInt(buf);

            // Used in case of data is spread over multiple buffers
            offset += buf.position() - pos;
        } else {
            // The size of the data is spread over multiple buffers
            int len = maxLen;
            int off = 0;
            sizeBuffer.reset();
            while (len > 0) {
                buf = getDataBuffer(offset + off);
                int count = Math.min(len, buf.remaining());
                buf.get(sizeBuffer.getBuf(), off, count);
                off += count;
                len -= count;
            }
            size = VarLengthIntUtils.decodeInt(sizeBuffer);
            offset += sizeBuffer.getPos();
            buf = getDataBuffer(offset);
        }

        // Create output bytes
        byte[] res = new byte[size];

        // Check if the data is one buffer
        if (buf.remaining() >= size) {
            // Continuous read
            buf.get(res, 0, size);
        } else {
            int len = size;
            int off = 0;
            while (len > 0) {
                buf = getDataBuffer(offset);
                int count = Math.min(len, buf.remaining());
                buf.get(res, off, count);
                offset += count;
                off += count;
                len -= count;
            }
        }

        return res;
    }

    // Get data from disk
    private byte[] getDiskBytes(long offset) throws IOException {
        mappedFile.seek(dataOffset + offset);

        // Get size of data
        int size = VarLengthIntUtils.decodeInt(mappedFile);

        // Create output bytes
        byte[] res = new byte[size];

        // Read data
        if (mappedFile.read(res) == -1) {
            throw new EOFException();
        }

        return res;
    }

    // Return the data buffer for the given position
    private ByteBuffer getDataBuffer(long index) {
        ByteBuffer buf = dataBuffers[(int) (index / mmpSegmentSize)];
        buf.position((int) (index % mmpSegmentSize));
        return buf;
    }

    private String formatCreatedAt(long createdAt) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd G 'at' HH:mm:ss z");
        Calendar cl = Calendar.getInstance();
        cl.setTimeInMillis(createdAt);
        return sdf.format(cl.getTime());
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
                indexBuffer.position(currentIndexOffset);

                long offset = 0;
                while (offset == 0) {
                    indexBuffer.get(currentSlotBuffer);
                    offset = VarLengthIntUtils.decodeLong(currentSlotBuffer, currentKeyLength);
                    currentIndexOffset += currentSlotBuffer.length;
                }

                byte[] key = Arrays.copyOf(currentSlotBuffer, currentKeyLength);
                byte[] value = null;

                if (withValue) {
                    long valueOffset = currentDataOffset + offset;
                    value = mMapData ? getMMapBytes(valueOffset) : getDiskBytes(valueOffset);
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
