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

package org.apache.paimon.fileindex.rangebitmap.dictionary.chunked;

import org.apache.paimon.fileindex.rangebitmap.dictionary.Dictionary;
import org.apache.paimon.fs.SeekableInputStream;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;

import static org.apache.paimon.utils.IOUtils.readFully;

/** Chunked implementation of {@link Dictionary}. */
public class ChunkedDictionary implements Dictionary {

    public static final byte CURRENT_VERSION = 1;

    private final int size;
    private final int offsetsLength;
    private final int chunksLength;
    private final int bodyOffset;
    private final SeekableInputStream in;
    private final KeyFactory factory;
    private final Comparator<Object> comparator;
    private final Chunk[] caches;

    private ByteBuffer offsets;
    private ByteBuffer chunks;

    public ChunkedDictionary(SeekableInputStream in, int offset, KeyFactory factory)
            throws IOException {
        in.seek(offset);
        byte[] headerLengthInBytes = new byte[Integer.BYTES];
        readFully(in, headerLengthInBytes);
        int headerLength = ByteBuffer.wrap(headerLengthInBytes).getInt();

        byte[] headerInBytes = new byte[headerLength];
        readFully(in, headerInBytes);
        ByteBuffer header = ByteBuffer.wrap(headerInBytes);

        byte version = header.get();
        if (version > CURRENT_VERSION) {
            throw new IllegalArgumentException(String.format("invalid version %d", version));
        }
        this.size = header.getInt();
        this.offsetsLength = header.getInt();
        this.chunksLength = header.getInt();
        this.factory = factory;
        this.comparator = factory.createComparator();
        this.in = in;
        this.bodyOffset = offset + Integer.BYTES + headerLength;
        this.caches = new Chunk[size];
    }

    @Override
    public int find(Object key) {
        int low = 0;
        int high = size - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            Chunk found = get(mid);
            int result = comparator.compare(found.key(), key);
            if (result > 0) {
                high = mid - 1;
            } else if (result < 0) {
                low = mid + 1;
            } else {
                return found.code();
            }
        }
        // key not found
        if (low == 0) {
            return -(low + 1);
        }
        return get(low - 1).find(key);
    }

    @Override
    public Object find(int code) {
        if (code < 0) {
            throw new IndexOutOfBoundsException("invalid code: " + code);
        }
        int low = 0;
        int high = size - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            Chunk found = get(mid);
            int result = Integer.compare(found.code(), code);
            if (result > 0) {
                high = mid - 1;
            } else if (result < 0) {
                low = mid + 1;
            } else {
                return found.key();
            }
        }
        return get(low - 1).find(code);
    }

    private Chunk get(int index) {
        if (caches[index] != null) {
            return caches[index];
        }

        if (offsets == null || chunks == null) {
            try {
                byte[] bytes = new byte[offsetsLength + chunksLength];
                in.seek(bodyOffset);
                readFully(in, bytes);

                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                offsets = (ByteBuffer) buffer.slice().limit(offsetsLength);

                buffer.position(offsetsLength);
                chunks = (ByteBuffer) buffer.slice().limit(chunksLength);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        offsets.position(index * Integer.BYTES);
        int offset = offsets.getInt();
        chunks.position(offset);
        ByteBuffer header =
                offsets.position() == offsets.limit()
                        ? (ByteBuffer) chunks.slice().limit(chunksLength - offset)
                        : (ByteBuffer) chunks.slice().limit(offsets.getInt() - offset);
        caches[index] = factory.mmapChunk(header, bodyOffset + offsetsLength + chunksLength, in);
        return caches[index];
    }

    /** A Builder for {@link ChunkedDictionary}. */
    public static class Appender implements Dictionary.Appender {

        public static final byte CURRENT_VERSION = 1;

        private final KeyFactory factory;
        private final Comparator<Object> comparator;
        private final int limitedSerializedSizeInBytes;
        private final ByteArrayOutputStream offsetsBos;
        private final DataOutputStream offsets;

        private final ByteArrayOutputStream chunksBos;
        private final DataOutputStream chunks;

        private final ByteArrayOutputStream keysBos;
        private final DataOutputStream keys;

        private Chunk chunk;
        private int chunksOffset;
        private int keysOffset;
        private int size;

        private Object key;
        private Integer code;

        public Appender(KeyFactory factory, int limitedSerializedSizeInBytes) {
            this.factory = factory;
            this.comparator = factory.createComparator();
            this.limitedSerializedSizeInBytes = limitedSerializedSizeInBytes;
            this.offsetsBos = new ByteArrayOutputStream();
            this.chunksBos = new ByteArrayOutputStream();
            this.keysBos = new ByteArrayOutputStream();
            this.offsets = new DataOutputStream(offsetsBos);
            this.chunks = new DataOutputStream(chunksBos);
            this.keys = new DataOutputStream(keysBos);
            this.chunksOffset = 0;
            this.keysOffset = 0;
        }

        @Override
        public void sortedAppend(Object key, int code) {
            if (key == null) {
                throw new IllegalArgumentException("key should not be null");
            }

            if (this.key != null && comparator.compare(this.key, key) >= 0) {
                throw new IllegalArgumentException("key can not out of order");
            } else {
                this.key = key;
            }

            if (this.code != null && this.code.compareTo(code) >= 0) {
                throw new IllegalArgumentException("code can not out of order");
            } else {
                this.code = code;
            }

            append(key, code);
        }

        @Override
        public byte[] serialize() {
            if (chunk != null) {
                flush();
            }

            int headerSize = 0;
            headerSize += Byte.BYTES; // version
            headerSize += Integer.BYTES; // size
            headerSize += Integer.BYTES; // offsets length
            headerSize += Integer.BYTES; // chunks length

            int bodySize = 0;
            bodySize += offsets.size();
            bodySize += chunks.size();
            bodySize += keys.size();

            ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + headerSize + bodySize);
            // write header length
            buffer.putInt(headerSize);

            // write header
            buffer.put(CURRENT_VERSION);
            buffer.putInt(size);
            buffer.putInt(offsets.size());
            buffer.putInt(chunks.size());

            // write body
            buffer.put(offsetsBos.toByteArray());
            buffer.put(chunksBos.toByteArray());
            buffer.put(keysBos.toByteArray());

            return buffer.array();
        }

        private void append(Object key, int code) {
            if (chunk == null) {
                chunk = factory.createChunk(key, code, keysOffset, limitedSerializedSizeInBytes);
            } else {
                if (chunk.tryAdd(key)) {
                    return;
                }
                flush();
                chunk = factory.createChunk(key, code, keysOffset, limitedSerializedSizeInBytes);
            }
        }

        private void flush() {
            try {
                byte[] chunkInBytes = chunk.serializeChunk();
                byte[] keysInBytes = chunk.serializeKeys();

                offsets.writeInt(chunksOffset);
                chunksOffset += chunkInBytes.length;
                keysOffset += keysInBytes.length;

                chunks.write(chunkInBytes);
                keys.write(keysInBytes);

                size += 1;
                chunk = null;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
