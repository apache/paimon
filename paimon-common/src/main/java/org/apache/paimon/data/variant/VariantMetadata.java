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

package org.apache.paimon.data.variant;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import static org.apache.paimon.utils.BinaryRowDataUtil.byteBufferEquals;

/**
 * The string dictionary of a variant metadata buffer, with lazily decoded entries.
 *
 * <p>Rows of a batch usually share the same metadata bytes, so an instance can be shared across
 * rows: {@link #adopt()} reparses the dictionary only when the bytes change. A shared instance is a
 * mutable cache slot, not a stable dictionary; do not retain it across calls.
 *
 * <p>This class is not thread safe.
 */
public class VariantMetadata {
    private static final int MAX_DICTIONARY_SIZE = 1 << 24;

    private ByteBuffer reuse;
    private ByteBuffer current;
    private int dictSize;
    private String[] dict;
    private int offsetSize;
    private int stringStart;

    /** Creates an empty metadata that adopts the first buffer passed to {@link #adopt}. */
    public static VariantMetadata empty() {
        return new VariantMetadata();
    }

    private VariantMetadata() {
        this.reuse = null;
        this.current = null;
        this.dictSize = 0;
        this.dict = null;
        this.offsetSize = 0;
        this.stringStart = 0;
    }

    public void setCurrent(ByteBuffer metadata) {
        this.current = metadata;
    }

    public void adopt() {
        if (current == null) {
            throw GenericVariantUtil.malformedVariant();
        }

        if (reuse != null
                && reuse.remaining() == current.remaining()
                && byteBufferEquals(reuse, current)) {
            return;
        }

        reuse = snapshot(current);

        offsetSize = ((GenericVariantUtil.getByte(reuse, 0) >> 6) & 0x3) + 1;
        dictSize = GenericVariantUtil.readUnsigned(reuse, 1, offsetSize);
        long offsetTableEnd = 1L + offsetSize + (dictSize + 1L) * offsetSize;
        if (dictSize > MAX_DICTIONARY_SIZE || offsetTableEnd > reuse.remaining()) {
            throw GenericVariantUtil.malformedVariant();
        }
        stringStart = 1 + (dictSize + 2) * offsetSize;

        if (dict == null || dict.length < dictSize) {
            dict = new String[dictSize];
        } else {
            Arrays.fill(dict, 0, dictSize, null);
        }
    }

    public int length() {
        return dictSize;
    }

    public String get(int id) {
        if (id < 0 || id >= dictSize) {
            throw GenericVariantUtil.malformedVariant();
        }
        String key = dict[id];
        if (key == null) {
            int start =
                    GenericVariantUtil.readUnsigned(reuse, 1 + (id + 1) * offsetSize, offsetSize);
            int nextStart =
                    GenericVariantUtil.readUnsigned(reuse, 1 + (id + 2) * offsetSize, offsetSize);
            if (nextStart < start) {
                throw GenericVariantUtil.malformedVariant();
            }
            if (nextStart > 0) {
                int end = stringStart + nextStart - 1;
                if (end < 0 || end >= reuse.remaining()) {
                    throw GenericVariantUtil.malformedVariant();
                }
            }
            int length = nextStart - start;
            key = GenericVariantUtil.decodeMetadataKey(reuse, stringStart + start, length);
            dict[id] = key;
        }
        return key;
    }

    public ByteBuffer buffer() {
        return current;
    }

    /** Snapshots the buffer's remaining bytes so later decodes do not observe reused memory. */
    private static ByteBuffer snapshot(ByteBuffer buffer) {
        ByteBuffer copy = buffer.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
    }
}
