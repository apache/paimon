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

package org.apache.paimon.tantivy.index;

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.tantivy.TantivySearcher;
import org.apache.paimon.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Pool of {@link TantivySearcher} instances keyed by index file identity ({@code filePath@size}).
 *
 * <p>Each searcher holds the Tantivy index open in Rust memory (including the FST term dictionary).
 * Pooling avoids the repeated cost of loading the index on every query.
 *
 * <p>Thread-safe. Borrow/return semantics guarantee at most one thread uses a given entry at a
 * time.
 */
public class TantivySearcherPool {

    /** A borrowed searcher + its backing stream. Both are returned together. */
    static final class PooledEntry implements Closeable {
        final TantivySearcher searcher;
        final SeekableInputStream stream;

        PooledEntry(TantivySearcher searcher, SeekableInputStream stream) {
            this.searcher = searcher;
            this.stream = stream;
        }

        @Override
        public void close() throws IOException {
            IOUtils.closeQuietly(searcher);
            IOUtils.closeQuietly(stream);
        }
    }

    private final int maxSizePerKey;
    private final ConcurrentHashMap<String, LinkedBlockingDeque<PooledEntry>> pool =
            new ConcurrentHashMap<>();

    public TantivySearcherPool(int maxSizePerKey) {
        this.maxSizePerKey = maxSizePerKey;
    }

    /**
     * Borrow an idle entry for the given key, or {@code null} if the pool has none.
     *
     * <p>The caller must either {@link #returnEntry} or {@link PooledEntry#close} the entry when
     * done.
     */
    @Nullable
    public PooledEntry borrow(String key) {
        LinkedBlockingDeque<PooledEntry> deque = pool.get(key);
        return deque == null ? null : deque.pollFirst();
    }

    /**
     * Return a previously borrowed entry. If the pool is full, the entry is closed immediately.
     *
     * <p>The stream position after use is irrelevant — Rust always seeks before reading.
     */
    public void returnEntry(String key, PooledEntry entry) {
        if (maxSizePerKey <= 0) {
            IOUtils.closeQuietly(entry);
            return;
        }
        LinkedBlockingDeque<PooledEntry> deque =
                pool.computeIfAbsent(key, k -> new LinkedBlockingDeque<>(maxSizePerKey));
        if (!deque.offerFirst(entry)) {
            IOUtils.closeQuietly(entry);
        }
    }
}
