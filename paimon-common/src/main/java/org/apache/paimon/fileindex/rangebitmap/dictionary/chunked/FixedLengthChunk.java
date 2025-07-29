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
import java.util.Arrays;
import java.util.Comparator;

/** Fixed length implementation of {@link Chunk}. */
public class FixedLengthChunk extends AbstractChunk {

    public static final byte CURRENT_VERSION = 1;

    private final byte version;
    private final Object key;
    private final int code;
    private final int offset;
    private final int fixedLength;

    private int size;
    private int keysBaseOffset;
    private int keysLength;
    private SeekableInputStream in;
    private ByteBuffer keys;
    private KeyFactory.KeySerializer serializer;
    private KeyFactory.KeyDeserializer deserializer;

    public FixedLengthChunk(
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
        this.keys = ByteBuffer.allocate(limitedSerializedSizeInBytes);
        this.serializer = serializer;
        this.fixedLength = this.serializer.serializedSizeInBytes(key);
    }

    public FixedLengthChunk(
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
        this.keysLength = headers.getInt();
        this.fixedLength = headers.getInt();
        this.keysBaseOffset = keysBaseOffset;
        this.in = in;
        this.deserializer = deserializer;
    }

    @Override
    protected int size() {
        return size;
    }

    @Override
    protected Object get(int index) {
        if (keys == null) {
            try {
                in.seek(keysBaseOffset + offset);
                byte[] bytes = new byte[keysLength];
                IOUtils.readFully(in, bytes);
                keys = ByteBuffer.wrap(bytes);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        keys.position(index * fixedLength);
        return deserializer.deserialize(keys);
    }

    @Override
    public boolean tryAdd(Object key) {
        if (fixedLength > keys.remaining()) {
            return false;
        }
        serializer.serialize(keys, key);
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
    public byte[] serializeChunk() {
        keys.flip();

        int serializedSizeInBytes = 0;
        serializedSizeInBytes += Byte.BYTES;
        serializedSizeInBytes += fixedLength; // key length
        serializedSizeInBytes += Integer.BYTES; // code
        serializedSizeInBytes += Integer.BYTES; // offset
        serializedSizeInBytes += Integer.BYTES; // size
        serializedSizeInBytes += Integer.BYTES; // keys length
        serializedSizeInBytes += Integer.BYTES; // fixed length

        ByteBuffer buffer = ByteBuffer.allocate(serializedSizeInBytes);
        buffer.put(version);
        serializer.serialize(buffer, key);
        buffer.putInt(code);
        buffer.putInt(offset);
        buffer.putInt(size);
        buffer.putInt(keys.limit());
        buffer.putInt(fixedLength);
        return buffer.array();
    }

    @Override
    public byte[] serializeKeys() {
        return Arrays.copyOf(keys.array(), keys.limit());
    }
}
