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

package org.apache.paimon.lookup.local;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.function.LongSupplier;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Value envelope which distinguishes LocalKvDb tombstones and optionally stores TTL.
 *
 * <p>TTL is intentionally not enforced while decoding. Like RocksDB TtlDB, LocalKvDb removes
 * expired values during compaction, so reads may temporarily return an expired value.
 */
final class LocalKvValueCodec {

    private static final byte VALUE_MARKER = 1;
    private static final byte TTL_VALUE_MARKER = 2;
    private static final int EXPIRATION_BYTES = Long.BYTES;

    private final long ttlMillis;
    private final LongSupplier currentTimeMillis;

    LocalKvValueCodec(@Nullable Duration ttl) {
        this(ttl, System::currentTimeMillis);
    }

    LocalKvValueCodec(@Nullable Duration ttl, LongSupplier currentTimeMillis) {
        this.currentTimeMillis = currentTimeMillis;
        if (ttl == null) {
            this.ttlMillis = -1;
        } else {
            this.ttlMillis = ttl.toMillis();
            checkArgument(ttlMillis > 0, "TTL must be greater than zero.");
        }
    }

    byte[] encode(byte[] value) {
        if (!ttlEnabled()) {
            byte[] stored = new byte[value.length + 1];
            stored[0] = VALUE_MARKER;
            System.arraycopy(value, 0, stored, 1, value.length);
            return stored;
        }

        byte[] stored = new byte[value.length + 1 + EXPIRATION_BYTES];
        stored[0] = TTL_VALUE_MARKER;
        long now = currentTimeMillis.getAsLong();
        long expiration = Long.MAX_VALUE - now < ttlMillis ? Long.MAX_VALUE : now + ttlMillis;
        writeLong(stored, 1, expiration);
        System.arraycopy(value, 0, stored, 1 + EXPIRATION_BYTES, value.length);
        return stored;
    }

    byte[] decode(byte[] stored) throws IOException {
        int valueOffset = valueOffset(stored, 0, stored.length);
        return Arrays.copyOfRange(stored, valueOffset, stored.length);
    }

    /** Return the encoded value offset. TTL expiration is enforced only during compaction. */
    int valueOffset(byte[] stored, int offset, int length) throws IOException {
        if (offset < 0
                || length <= 0
                || offset > stored.length
                || length > stored.length - offset) {
            throw new IOException("Corrupted LocalKvState value marker.");
        }

        if (!ttlEnabled()) {
            if (stored[offset] != VALUE_MARKER) {
                throw new IOException("Corrupted LocalKvState value marker.");
            }
            return offset + 1;
        }

        if (length < 1 + EXPIRATION_BYTES || stored[offset] != TTL_VALUE_MARKER) {
            throw new IOException("Corrupted LocalKvState TTL value.");
        }
        return offset + 1 + EXPIRATION_BYTES;
    }

    boolean ttlEnabled() {
        return ttlMillis > 0;
    }

    boolean isExpired(byte[] stored) {
        return stored.length >= 1 + EXPIRATION_BYTES
                && stored[0] == TTL_VALUE_MARKER
                && currentTimeMillis.getAsLong() >= readLong(stored, 1);
    }

    private static void writeLong(byte[] bytes, int offset, long value) {
        for (int i = Long.BYTES - 1; i >= 0; i--) {
            bytes[offset + i] = (byte) value;
            value >>>= Byte.SIZE;
        }
    }

    private static long readLong(byte[] bytes, int offset) {
        long value = 0;
        for (int i = 0; i < Long.BYTES; i++) {
            value = (value << Byte.SIZE) | (bytes[offset + i] & 0xffL);
        }
        return value;
    }
}
