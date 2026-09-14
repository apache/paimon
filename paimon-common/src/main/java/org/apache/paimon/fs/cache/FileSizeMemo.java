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

package org.apache.paimon.fs.cache;

import java.util.Iterator;
import java.util.LinkedHashMap;

/**
 * Least-recently-used memo of file sizes, bounded by entry count.
 *
 * <p>A memo is tiny but there is one per path, so an unbounded map grows with the number of
 * distinct files a long-lived process reads. Losing one only costs the extra {@code getFileStatus}
 * that would have been made anyway, and the files these caches accept are immutable, so a re-read
 * returns the same size.
 *
 * <p>Not thread-safe: callers hold their own lock. Access order means {@link #get} mutates the map,
 * so even a read has to be inside it.
 */
class FileSizeMemo {

    private static final int MAX_ENTRIES = 65536;

    private final LinkedHashMap<String, Long> sizes = new LinkedHashMap<>(64, 0.75f, true);

    /** Read through a method, not the constant: a constant is inlined into the test's bytecode. */
    static int maxEntries() {
        return MAX_ENTRIES;
    }

    /** Entry count, so a test can observe the bound without reading an entry. */
    int size() {
        return sizes.size();
    }

    long get(String filePath) {
        Long size = sizes.get(filePath);
        return size != null ? size : -1;
    }

    void put(String filePath, long size) {
        sizes.put(filePath, size);
        Iterator<String> iterator = sizes.keySet().iterator();
        while (sizes.size() > MAX_ENTRIES && iterator.hasNext()) {
            iterator.next();
            iterator.remove();
        }
    }

    void invalidate(String filePathPrefix) {
        sizes.keySet().removeIf(filePath -> filePath.startsWith(filePathPrefix));
    }
}
