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

import java.util.Comparator;

/** Common implementation of {@link Chunk}. */
public abstract class AbstractChunk implements Chunk {

    private final Comparator<Object> comparator;

    public AbstractChunk(Comparator<Object> comparator) {
        this.comparator = comparator;
    }

    @Override
    public int find(Object key) {
        if (comparator.compare(key(), key) == 0) {
            return code();
        }
        int low = 0;
        int high = size() - 1;
        int base = code() + 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            int result = comparator.compare(get(mid), key);
            if (result > 0) {
                high = mid - 1;
            } else if (result < 0) {
                low = mid + 1;
            } else {
                return base + mid;
            }
        }
        return -(base + low + 1);
    }

    @Override
    public Object find(int code) {
        int current = code();
        if (current == code) {
            return key();
        }
        int index = code - current - 1;
        if (index < 0 || index >= size()) {
            throw new IndexOutOfBoundsException("invalid code: " + code);
        }
        return get(index);
    }

    protected abstract int size();

    protected abstract Object get(int index);
}
