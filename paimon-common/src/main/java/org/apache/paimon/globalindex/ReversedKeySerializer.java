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

package org.apache.paimon.globalindex;

import org.apache.paimon.memory.MemorySlice;

import java.util.Comparator;

/** A {@link KeySerializer} that reverses serialized key bytes. */
public class ReversedKeySerializer implements KeySerializer {

    private final KeySerializer delegate;

    public ReversedKeySerializer(KeySerializer delegate) {
        this.delegate = delegate;
    }

    @Override
    public byte[] serialize(Object key) {
        return reverse(delegate.serialize(key));
    }

    @Override
    public Object deserialize(MemorySlice data) {
        return delegate.deserialize(MemorySlice.wrap(reverse(data.copyBytes())));
    }

    @Override
    public Comparator<Object> createComparator() {
        return (left, right) -> compareUnsigned(serialize(left), serialize(right));
    }

    private static byte[] reverse(byte[] bytes) {
        byte[] out = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            out[i] = bytes[bytes.length - 1 - i];
        }
        return out;
    }

    private static int compareUnsigned(byte[] left, byte[] right) {
        int len = Math.min(left.length, right.length);
        for (int i = 0; i < len; i++) {
            int cmp = (left[i] & 0xFF) - (right[i] & 0xFF);
            if (cmp != 0) {
                return cmp;
            }
        }
        return left.length - right.length;
    }
}
