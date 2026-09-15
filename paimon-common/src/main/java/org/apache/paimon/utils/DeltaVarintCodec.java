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

/** Streaming unsigned delta/varint encoding for nondecreasing, nonnegative long values. */
public final class DeltaVarintCodec {
    private DeltaVarintCodec() {}

    /** Writes each delta immediately. The caller owns the output and stores the value count. */
    public static final class Writer {
        private final DataOutput out;
        private long previous;

        public Writer(DataOutput out, long base) {
            if (base < 0) {
                throw new IllegalArgumentException("Delta base must be nonnegative");
            }
            this.out = Objects.requireNonNull(out);
            previous = base;
        }

        public void write(long value) throws IOException {
            require(value >= previous);
            VarLengthIntUtils.encodeLong(out, value - previous);
            previous = value;
        }
    }

    /**
     * Reads values on demand and advances the supplied buffer. The buffer must contain exactly the
     * encoded sequence; a complete read validates its boundary. Reading may stop early.
     */
    public static final class Reader {
        private final ByteBuffer data;
        private final long max;
        private long remaining;
        private long value;

        public Reader(ByteBuffer data, long count, long base, long max) throws IOException {
            this.data = Objects.requireNonNull(data);
            require(count >= 0 && count <= data.remaining() && base >= 0 && max >= base);
            require(count != 0 || !data.hasRemaining());
            remaining = count;
            value = base;
            this.max = max;
        }

        public boolean hasNext() {
            return remaining > 0;
        }

        public long next() throws IOException {
            require(remaining > 0);
            long delta = VarLengthIntUtils.decodeLong(data);
            require(delta <= max - value);
            value += delta;
            remaining--;
            require(remaining != 0 || !data.hasRemaining());
            return value;
        }
    }

    private static void require(boolean valid) throws IOException {
        if (!valid) {
            throw new IOException("Invalid delta/varint sequence");
        }
    }
}
