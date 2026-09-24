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

package org.apache.paimon.utils;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/** Streaming count-prefixed delta/varint encoding of nonnegative integer sequences. */
public final class DeltaVarintCodec {

    private DeltaVarintCodec() {}

    /** Writes a varint count followed by deltas, without buffering the sequence. */
    public static final class Writer {
        private final DataOutput out;
        private final boolean signedDeltas;
        private long remaining;
        private long previous;

        public Writer(DataOutput out, int count, long base) throws IOException {
            this(out, count, base, false);
        }

        /** Signed deltas support non-monotonic nonnegative int values using ZigZag. */
        public Writer(DataOutput out, int count, long base, boolean signedDeltas)
                throws IOException {
            if (base < 0 || count < 0 || (signedDeltas && base > Integer.MAX_VALUE)) {
                throw new IllegalArgumentException("Invalid delta/varint count or base");
            }
            this.out = Objects.requireNonNull(out);
            this.signedDeltas = signedDeltas;
            this.remaining = count;
            previous = base;
            VarLengthIntUtils.encodeInt(out, count);
        }

        public void write(long value) throws IOException {
            require(remaining > 0 && value >= 0);
            long delta = value - previous;
            if (signedDeltas) {
                require(value <= Integer.MAX_VALUE);
                delta = (delta << 1) ^ (delta >> 63);
            } else {
                require(delta >= 0);
            }
            VarLengthIntUtils.encodeLong(out, delta);
            previous = value;
            remaining--;
        }
    }

    /**
     * Reads the count and then values on demand. A complete read leaves the buffer at the next
     * payload, allowing count-prefixed sequences to be concatenated. Reading may stop early.
     */
    public static final class Reader {
        private final ByteBuffer data;
        private final long max;
        private final int count;
        private final boolean signedDeltas;
        private long remaining;
        private long value;

        public Reader(ByteBuffer data, long base, long max) throws IOException {
            this(data, base, max, false);
        }

        public Reader(ByteBuffer data, long base, long max, boolean signedDeltas)
                throws IOException {
            this.data = Objects.requireNonNull(data);
            long encodedCount = VarLengthIntUtils.decodeLong(data);
            require(encodedCount <= Integer.MAX_VALUE && encodedCount <= data.remaining());
            require(base >= 0 && max >= base && (!signedDeltas || max <= Integer.MAX_VALUE));
            count = (int) encodedCount;
            remaining = count;
            value = base;
            this.max = max;
            this.signedDeltas = signedDeltas;
        }

        public int count() {
            return count;
        }

        public boolean hasNext() {
            return remaining > 0;
        }

        public long next() throws IOException {
            require(remaining > 0);
            long delta = VarLengthIntUtils.decodeLong(data);
            if (signedDeltas) {
                require(delta <= 2L * Integer.MAX_VALUE);
                delta = (delta >>> 1) ^ -(delta & 1);
            }
            require(delta >= -value && delta <= max - value);
            value += delta;
            remaining--;
            return value;
        }
    }

    private static void require(boolean valid) throws IOException {
        if (!valid) {
            throw new IOException("Invalid delta/varint sequence");
        }
    }
}
