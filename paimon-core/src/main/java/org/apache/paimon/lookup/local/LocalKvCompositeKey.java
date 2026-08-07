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

import org.apache.paimon.memory.MemorySlice;

import javax.annotation.Nullable;

import java.util.Arrays;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Composite-key helpers for prefix-free serialized logical keys.
 *
 * <p>This matches the existing RocksDB state layout and relies on the serialized logical key being
 * prefix-free. Paimon's lookup states use length-delimited row keys, and fixed-width primitive
 * serializers such as integers are prefix-free as well.
 */
final class LocalKvCompositeKey {

    private LocalKvCompositeKey() {}

    static byte[] prefix(byte[] key) {
        return Arrays.copyOf(key, key.length);
    }

    static byte[] append(byte[] prefix, byte[] suffix) {
        byte[] result = Arrays.copyOf(prefix, prefix.length + suffix.length);
        System.arraycopy(suffix, 0, result, prefix.length, suffix.length);
        return result;
    }

    static byte[] appendLong(byte[] prefix, long suffix) {
        checkArgument(suffix >= 0, "Composite-key sequence must be non-negative.");
        byte[] result = Arrays.copyOf(prefix, prefix.length + Long.BYTES);
        for (int i = result.length - 1; i >= prefix.length; i--) {
            result[i] = (byte) suffix;
            suffix >>>= Byte.SIZE;
        }
        return result;
    }

    @Nullable
    static byte[] upperBound(byte[] prefix) {
        byte[] result = Arrays.copyOf(prefix, prefix.length);
        for (int i = result.length - 1; i >= 0; i--) {
            int value = result[i] & 0xff;
            if (value != 0xff) {
                result[i] = (byte) (value + 1);
                return Arrays.copyOf(result, i + 1);
            }
        }
        return null;
    }

    static byte[] suffix(byte[] compositeKey, int prefixLength) {
        checkArgument(
                prefixLength <= compositeKey.length,
                "Composite key is shorter than its logical-key prefix.");
        return Arrays.copyOfRange(compositeKey, prefixLength, compositeKey.length);
    }

    static byte[] suffix(MemorySlice compositeKey, int prefixLength) {
        checkArgument(
                prefixLength <= compositeKey.length(),
                "Composite key is shorter than its logical-key prefix.");
        return compositeKey.slice(prefixLength, compositeKey.length() - prefixLength).copyBytes();
    }
}
