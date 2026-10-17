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

import org.apache.paimon.annotation.VisibleForTesting;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.apache.paimon.utils.BinaryRowDataUtil.byteBufferEquals;
import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * The variant metadata dictionary, decoded lazily on first use.
 *
 * <p>Rows of a batch usually share the same metadata bytes, so an instance can be shared across
 * rows and reparse the dictionary only when the bytes change. A shared instance is a mutable cache
 * slot, not a stable dictionary; do not retain it across calls.
 *
 * <p>{@link #adopt} adopts the current row's metadata; {@link #get} decodes the dictionary lazily
 * on first use, while {@link #metadataBuffer()} returns the raw bytes without decoding. A fully
 * typed projection never touches the dictionary, so it pays nothing per row.
 *
 * <p>Not thread safe.
 */
public class VariantMetadata {
    private static final int MAX_DICTIONARY_SIZE = 1 << 24;

    // Current row's metadata bytes; a live reference valid only during the synchronous read.
    private ByteBuffer raw;
    // Decoded metadata snapshot, read by {@link #get}.
    private ByteBuffer snapshot;
    // Whether {@link #snapshot} already matches the current {@link #raw}.
    private boolean decoded;
    private int dictSize;
    private String[] dict;
    private int offsetSize;
    private int stringStart;

    /**
     * Creates an empty metadata; the dictionary is decoded lazily from the buffer given to {@link
     * #adopt}.
     */
    public static VariantMetadata empty() {
        return new VariantMetadata();
    }

    private VariantMetadata() {
        this.raw = null;
        this.snapshot = null;
        this.decoded = false;
        this.dictSize = 0;
        this.dict = null;
        this.offsetSize = 0;
        this.stringStart = 0;
    }

    /**
     * Adopt the current row's metadata bytes. The decode is deferred until {@link #get} (or {@link
     * #dictionarySize}) is first called. Call once per row before any read that may need the
     * dictionary.
     */
    public void adopt(ByteBuffer raw) {
        this.raw = raw;
        this.decoded = false;
    }

    public ByteBuffer metadataBuffer() {
        checkState(
                raw != null,
                "metadata must be adopted before use; call VariantMetadata.adopt(...) first");
        return raw;
    }

    @VisibleForTesting
    boolean isDecoded() {
        return decoded;
    }

    private void ensureDecoded() {
        if (decoded) {
            return;
        }
        if (raw != null) {
            decode(raw);
        } else if (snapshot == null) {
            throw new IllegalStateException(
                    "metadata must be adopted before use; call VariantMetadata.adopt(...) first");
        }
        decoded = true;
    }

    @VisibleForTesting
    int dictionarySize() {
        ensureDecoded();
        return dictSize;
    }

    public String get(int id) {
        ensureDecoded();
        if (id < 0 || id >= dictSize) {
            throw GenericVariantUtil.malformedVariant();
        }
        String key = dict[id];
        if (key == null) {
            int start =
                    GenericVariantUtil.readUnsigned(
                            snapshot, 1 + (id + 1) * offsetSize, offsetSize);
            int nextStart =
                    GenericVariantUtil.readUnsigned(
                            snapshot, 1 + (id + 2) * offsetSize, offsetSize);
            if (nextStart < start) {
                throw GenericVariantUtil.malformedVariant();
            }
            if (nextStart > 0) {
                int end = stringStart + nextStart - 1;
                if (end < 0 || end >= snapshot.remaining()) {
                    throw GenericVariantUtil.malformedVariant();
                }
            }
            int length = nextStart - start;
            key = GenericVariantUtil.decodeMetadataKey(snapshot, stringStart + start, length);
            dict[id] = key;
        }
        return key;
    }

    // Decodes {@link #raw} into the cached dictionary. Called only from {@link #ensureDecoded}.
    private void decode(ByteBuffer raw) {
        if (snapshot != null
                && snapshot.remaining() == raw.remaining()
                && byteBufferEquals(snapshot, raw)) {
            return;
        }

        // Validate into locals first; only publish after the checks pass.
        int offsetSize = ((GenericVariantUtil.getByte(raw, 0) >> 6) & 0x3) + 1;
        int dictSize = GenericVariantUtil.readUnsigned(raw, 1, offsetSize);
        long offsetTableEnd = 1L + offsetSize + (dictSize + 1L) * offsetSize;
        if (dictSize > MAX_DICTIONARY_SIZE || offsetTableEnd > raw.remaining()) {
            throw GenericVariantUtil.malformedVariant();
        }

        snapshot = copyBuffer(raw);
        this.offsetSize = offsetSize;
        this.dictSize = dictSize;
        stringStart = 1 + (dictSize + 2) * offsetSize;

        if (dict == null || dict.length < dictSize) {
            dict = new String[dictSize];
        } else {
            Arrays.fill(dict, 0, dictSize, null);
        }
    }

    private static ByteBuffer copyBuffer(ByteBuffer buffer) {
        ByteBuffer copy = buffer.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return ByteBuffer.wrap(bytes).order(buffer.order());
    }
}
