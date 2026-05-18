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

package org.apache.paimon.lumina.index;

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.utils.IOUtils;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Pool of {@link LuminaIndex} instances keyed by index file identity ({@code filePath@size}).
 *
 * <p>Each entry holds the Lumina index open in native memory (including the loaded graph files),
 * so repeated queries on the same shard avoid reloading the index from object storage.
 *
 * <p>At most one idle entry is kept per key. Under concurrent queries on the same shard, the last
 * entry returned wins; displaced entries are closed automatically via the removal listener. The
 * total number of idle entries across all keys is bounded by {@code maxSize}.
 *
 * <p>Thread-safe. Borrow/return semantics guarantee at most one thread uses a given entry at a
 * time.
 */
public class LuminaSearcherPool {

    static final long EXPIRE_AFTER_ACCESS_MINUTES = 30;

    /** A borrowed searcher together with its backing stream and JNI bridge. */
    static final class PooledEntry implements Closeable {
        final LuminaIndex index;
        final SeekableInputStream stream;
        final LuminaVectorGlobalIndexReader.InputStreamFileInput fileInput;
        final LuminaIndexMeta indexMeta;

        PooledEntry(
                LuminaIndex index,
                SeekableInputStream stream,
                LuminaVectorGlobalIndexReader.InputStreamFileInput fileInput,
                LuminaIndexMeta indexMeta) {
            this.index = index;
            this.stream = stream;
            this.fileInput = fileInput;
            this.indexMeta = indexMeta;
        }

        @Override
        public void close() throws IOException {
            IOUtils.closeQuietly(index);
            IOUtils.closeQuietly(stream);
        }
    }

    @Nullable private final Cache<String, PooledEntry> idleCache;

    public LuminaSearcherPool(int maxSize) {
        if (maxSize <= 0) {
            this.idleCache = null;
        } else {
            this.idleCache =
                    Caffeine.newBuilder()
                            .maximumSize(maxSize)
                            .expireAfterAccess(EXPIRE_AFTER_ACCESS_MINUTES, TimeUnit.MINUTES)
                            .executor(Runnable::run)
                            .removalListener((k, v, c) -> IOUtils.closeQuietly((PooledEntry) v))
                            .build();
        }
    }

    /**
     * Borrow an idle entry for the given key, or {@code null} if the pool has none.
     *
     * <p>The caller must either {@link #returnEntry} or {@link PooledEntry#close} the entry when
     * done.
     */
    @Nullable
    public PooledEntry borrow(String key) {
        if (idleCache == null) {
            return null;
        }
        return idleCache.asMap().remove(key);
    }

    /**
     * Return a previously borrowed entry to the pool. Any entry displaced by size eviction, TTL
     * expiry, or key replacement is closed automatically via the removal listener.
     */
    public void returnEntry(String key, PooledEntry entry) {
        if (idleCache == null) {
            IOUtils.closeQuietly(entry);
            return;
        }
        idleCache.put(key, entry);
    }
}
