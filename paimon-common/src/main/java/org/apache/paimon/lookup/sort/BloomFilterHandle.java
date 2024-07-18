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

package org.apache.paimon.lookup.sort;

import java.util.Objects;

/** Handle for bloom filter. */
public class BloomFilterHandle {

    public static final int MAX_ENCODED_LENGTH = 9 + 5 + 9;

    private final long offset;
    private final int size;
    private final long expectedEntries;

    BloomFilterHandle(long offset, int size, long expectedEntries) {
        this.offset = offset;
        this.size = size;
        this.expectedEntries = expectedEntries;
    }

    public long offset() {
        return offset;
    }

    public int size() {
        return size;
    }

    public long expectedEntries() {
        return expectedEntries;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BloomFilterHandle that = (BloomFilterHandle) o;
        return offset == that.offset
                && size == that.size
                && expectedEntries == that.expectedEntries;
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, size, expectedEntries);
    }
}
