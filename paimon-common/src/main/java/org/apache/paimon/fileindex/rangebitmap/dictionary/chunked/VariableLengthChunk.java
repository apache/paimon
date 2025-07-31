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

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.utils.IOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;

/** Variable length implementation of {@link Chunk}. */
public class VariableLengthChunk extends AbstractChunk {

    public static final byte CURRENT_VERSION = 1;

    private final byte version;
    private final Object key;
    private final int code;
    private final int offset;

    private int size;
    private int currentOffset;

    private int keysBaseOffset;
    private int offsetsLength;
    private int keysLength;
    private SeekableInputStream in;
    private ByteBuffer offsets;
    private ByteBuffer keys;

    private KeyFactory.KeySerializer serializer;
    private KeyFactory.KeyDeserializer deserializer;

    public VariableLengthChunk(
            Object key,
            int code,
            int offset,
            int limitedSerializedSizeInBytes,
            KeyFactory.KeySerializer serializer,
            Comparator<Object> comparator) {
        super(comparator);
        this.version = CURRENT_VERSION;
        this.key = key;
        this.code = code;
        this.offset = offset;
        this.size = 0;
        this.currentOffset = 0;
        this.serializer = serializer;
        this.offsets = ByteBuffer.allocate(limitedSerializedSizeInBytes);
        this.keys = ByteBuffer.allocate(limitedSerializedSizeInBytes);
    }

    public VariableLengthChunk(
            ByteBuffer headers,
            int keysBaseOffset,
            SeekableInputStream in,
            KeyFactory.KeyDeserializer deserializer,
            Comparator<Object> comparator) {
        super(comparator);
        this.version = headers.get();
        if (version > CURRENT_VERSION) {
            throw new IllegalArgumentException("version out of range");
        }
        this.key = deserializer.deserialize(headers);
        this.code = headers.getInt();
        this.offset = headers.getInt();
        this.size = headers.getInt();
        this.offsetsLength = headers.getInt();
        this.keysLength = headers.getInt();
        this.keysBaseOffset = keysBaseOffset;
        this.in = in;
        this.deserializer = deserializer;
    }

    @Override
    public boolean tryAdd(Object key) {
        int length = serializer.serializedSizeInBytes(key);
        if (length > keys.remaining() || Integer.BYTES > offsets.remaining()) {
            return false;
        }
        offsets.putInt(currentOffset);
        serializer.serialize(keys, key);
        currentOffset += length;
        size += 1;
        return true;
    }

    @Override
    public Object key() {
        return key;
    }

    @Override
    public int code() {
        return code;
    }

    @Override
    protected int size() {
        return size;
    }

    @Override
    protected Object get(int index) {
        if (offsets == null || keys == null) {
            try {
                in.seek(keysBaseOffset + offset);
                byte[] bytes = new byte[offsetsLength + keysLength];
                IOUtils.readFully(in, bytes);

                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                offsets = (ByteBuffer) buffer.slice().limit(offsetsLength);

                buffer.position(offsetsLength);
                keys = (ByteBuffer) buffer.slice().limit(keysLength);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        offsets.position(index * Integer.BYTES);
        keys.position(offsets.getInt());
        return deserializer.deserialize(keys);
    }

    @Override
    public byte[] serializeChunk() {
        offsets.flip();
        keys.flip();

        int serializedSizeInBytes = 0;
        serializedSizeInBytes += Byte.BYTES;
        serializedSizeInBytes += serializer.serializedSizeInBytes(key); // key
        serializedSizeInBytes += Integer.BYTES; // code
        serializedSizeInBytes += Integer.BYTES; // offset
        serializedSizeInBytes += Integer.BYTES; // size
        serializedSizeInBytes += Integer.BYTES; // offsets length
        serializedSizeInBytes += Integer.BYTES; // keys length

        ByteBuffer buffer = ByteBuffer.allocate(serializedSizeInBytes);
        buffer.put(version);
        serializer.serialize(buffer, key);
        buffer.putInt(code);
        buffer.putInt(offset);
        buffer.putInt(size);
        buffer.putInt(offsets.limit());
        buffer.putInt(keys.limit());
        return buffer.array();
    }

    @Override
    public byte[] serializeKeys() {
        ByteBuffer buffer = ByteBuffer.allocate(offsets.limit() + keys.limit());
        buffer.put(offsets);
        buffer.put(keys);
        return buffer.array();
    }
}
