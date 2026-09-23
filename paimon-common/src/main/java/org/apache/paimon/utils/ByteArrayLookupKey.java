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

import javax.annotation.Nullable;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Reusable lookup view compatible with {@link ByteArrayKey}.
 *
 * <p>This object is mutable and must not be stored in a hash collection. The byte array is not
 * copied and only needs to remain unchanged for the duration of a lookup.
 */
public final class ByteArrayLookupKey {

    private @Nullable byte[] bytes;
    private int offset;
    private int length;
    private int hash;

    public ByteArrayLookupKey() {}

    public ByteArrayLookupKey(byte[] bytes) {
        reset(bytes);
    }

    public void reset(byte[] bytes) {
        checkArgument(bytes != null, "Byte array cannot be null.");
        reset(bytes, 0, bytes.length);
    }

    public void reset(byte[] bytes, int offset, int length) {
        checkArgument(bytes != null, "Byte array cannot be null.");
        checkArgument(offset >= 0 && length >= 0 && offset <= bytes.length - length);
        this.bytes = bytes;
        this.offset = offset;
        this.length = length;
        int hash = 1;
        for (int i = offset; i < offset + length; i++) {
            hash = 31 * hash + bytes[i];
        }
        this.hash = hash;
    }

    public void clear() {
        bytes = null;
        offset = 0;
        length = 0;
        hash = 0;
    }

    @Nullable
    byte[] bytes() {
        return bytes;
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this
                || (bytes != null
                        && obj instanceof ByteArrayKey
                        && equals(((ByteArrayKey) obj).bytes()))
                || (bytes != null
                        && obj instanceof ByteArrayLookupKey
                        && equals((ByteArrayLookupKey) obj));
    }

    @Override
    public int hashCode() {
        return hash;
    }

    private boolean equals(byte[] other) {
        if (length != other.length) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            if (bytes[offset + i] != other[i]) {
                return false;
            }
        }
        return true;
    }

    private boolean equals(ByteArrayLookupKey other) {
        if (other.bytes == null || length != other.length) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            if (bytes[offset + i] != other.bytes[other.offset + i]) {
                return false;
            }
        }
        return true;
    }
}
