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

package org.apache.paimon.io.cache;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.cache.Cache.CacheValue;
import org.apache.paimon.io.cache.CacheKey.PositionCacheKey;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.utils.ExceptionUtils;
import org.apache.paimon.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Cache manager to cache bytes to paged {@link MemorySegment}s. */
public class CacheManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CacheManager.class);

    private static final CacheCallback NO_CALLBACK = key -> {};

    // Only loading/removal touches this index. Hits use the shared cache directly. The links are
    // owned by the manager, never by a reader, and empty file entries are removed immediately.
    private final Map<Path, FilePages> files = new HashMap<>();

    private final Cache dataCache;
    private final Cache indexCache;
    private final long maxDataCacheBytes;
    private final long maxIndexCacheBytes;
    private final boolean offHeap;

    public CacheManager(MemorySize maxMemorySize, double highPriorityPoolRatio) {
        this(maxMemorySize, highPriorityPoolRatio, false);
    }

    private CacheManager(MemorySize maxMemorySize, double highPriorityPoolRatio, boolean offHeap) {
        Preconditions.checkArgument(
                highPriorityPoolRatio >= 0 && highPriorityPoolRatio < 1,
                "The high priority pool ratio should in the range [0, 1).");
        MemorySize indexCacheSize =
                MemorySize.ofBytes((long) (maxMemorySize.getBytes() * highPriorityPoolRatio));
        MemorySize dataCacheSize =
                MemorySize.ofBytes((long) (maxMemorySize.getBytes() * (1 - highPriorityPoolRatio)));
        this.maxDataCacheBytes = dataCacheSize.getBytes();
        this.maxIndexCacheBytes =
                highPriorityPoolRatio == 0 ? maxDataCacheBytes : indexCacheSize.getBytes();
        this.dataCache =
                CacheBuilder.newBuilder().maximumWeight(dataCacheSize).build(this::onRemoval);
        if (highPriorityPoolRatio == 0) {
            this.indexCache = dataCache;
        } else {
            this.indexCache =
                    CacheBuilder.newBuilder().maximumWeight(indexCacheSize).build(this::onRemoval);
        }
        this.offHeap = offHeap;
        LOG.info(
                "Initialize {} cache manager with data cache of {} and index cache of {}.",
                offHeap ? "off-heap" : "heap",
                dataCacheSize,
                indexCacheSize);
    }

    public static CacheManager createOffHeap(
            MemorySize maxMemorySize, double highPriorityPoolRatio) {
        return new CacheManager(maxMemorySize, highPriorityPoolRatio, true);
    }

    @VisibleForTesting
    public Cache dataCache() {
        return dataCache;
    }

    @VisibleForTesting
    public Cache indexCache() {
        return indexCache;
    }

    /** Whether a page fits its configured pool; this does not guarantee admission. */
    public boolean canFitPage(int pageSize, boolean isIndex) {
        return pageSize <= (isIndex ? maxIndexCacheBytes : maxDataCacheBytes);
    }

    /** Returns a cached decoded range, or null if a load is needed. */
    @Nullable
    public MemorySlice getPageSliceIfPresent(CacheKey key) {
        CacheValue value = (key.isIndex() ? indexCache : dataCache).getIfPresent(key);
        return value == null ? null : value.slice;
    }

    /** Returns a cached page without retaining a loader on the hit path. */
    @Nullable
    public MemorySegment getPageIfPresent(CacheKey key) {
        MemorySlice slice = getPageSliceIfPresent(key);
        return slice == null ? null : asSegment(slice);
    }

    public MemorySegment getPage(CacheKey key, CacheReader reader) {
        return getPage(key, reader, NO_CALLBACK);
    }

    public MemorySegment getPage(CacheKey key, CacheReader reader, CacheCallback callback) {
        return asSegment(getPageSlice(key, reader, MemorySlice::wrap, callback));
    }

    private static MemorySegment asSegment(MemorySlice slice) {
        if (slice.offset() == 0 && slice.length() == slice.segment().size()) {
            return slice.segment();
        }
        // The same key may have been loaded through the slice API. Do not expose framing bytes
        // outside the decoded range through the original whole-segment API.
        return MemorySegment.wrap(slice.copyBytes());
    }

    /** Caches the decoded range, allowing an uncompressed block to share its read buffer. */
    public MemorySlice getPageSlice(
            CacheKey key, CacheReader reader, Function<byte[], MemorySlice> decoder) {
        return getPageSlice(key, reader, decoder, NO_CALLBACK);
    }

    private MemorySlice getPageSlice(
            CacheKey key,
            CacheReader reader,
            Function<byte[], MemorySlice> decoder,
            CacheCallback callback) {
        Cache cache = key.isIndex() ? indexCache : dataCache;
        Cache.CacheValue value =
                cache.get(
                        key,
                        k -> {
                            try {
                                Page page =
                                        new Page(
                                                key,
                                                toMemorySlice(decoder.apply(reader.read(key))),
                                                callback);
                                register(page);
                                return page;
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
        return checkNotNull(value, "Cache result for key(%s) is null", key).slice;
    }

    public boolean contains(CacheKey key) {
        if (key.isIndex()) {
            return indexCache.contains(key);
        } else {
            return dataCache.contains(key);
        }
    }

    public void invalidPage(CacheKey key) {
        if (key.isIndex()) {
            indexCache.invalidate(key);
        } else {
            dataCache.invalidate(key);
        }
    }

    /**
     * Invalidates this file's registered pages, including pages loaded by other readers. Concurrent
     * reads may repopulate the cache; callers must stop reads before deleting or replacing a file.
     */
    public void invalidFile(Path filePath) throws IOException {
        List<Page> pages = new ArrayList<>();
        synchronized (files) {
            FilePages file = files.remove(filePath);
            if (file == null) {
                return;
            }
            Page page = file.head;
            while (page != null) {
                pages.add(page);
                Page next = page.next;
                page.owner = null;
                page.previous = null;
                page.next = null;
                page = next;
            }
            file.head = null;
        }

        // Never call the cache while holding the file index lock: loaders and removal listeners
        // acquire that lock from inside Caffeine. Delete by identity so an old invalidation cannot
        // delete a replacement loaded for the same key.
        Throwable collected = null;
        for (Page page : pages) {
            try {
                invalidPage(page.key, page);
            } catch (Throwable t) {
                collected = ExceptionUtils.firstOrSuppressed(t, collected);
            }
        }
        if (collected != null) {
            if (collected instanceof Error) {
                throw (Error) collected;
            }
            if (collected instanceof RuntimeException) {
                throw (RuntimeException) collected;
            }
            throw new IOException(collected);
        }
    }

    /** Invalidates a particular page without removing a newer value for the same key. */
    protected void invalidPage(CacheKey key, CacheValue expected) {
        (key.isIndex() ? indexCache : dataCache).asMap().remove(key, expected);
    }

    private void register(Page page) {
        if (!(page.key instanceof PositionCacheKey)) {
            return;
        }
        Path path = ((PositionCacheKey) page.key).filePath();
        synchronized (files) {
            FilePages file = files.computeIfAbsent(path, ignored -> new FilePages(path));
            page.owner = file;
            page.next = file.head;
            if (file.head != null) {
                file.head.previous = page;
            }
            file.head = page;
        }
    }

    private void onRemoval(CacheKey key, CacheValue value) {
        if (value == null) {
            return;
        }
        if (value instanceof Page) {
            Page page = (Page) value;
            synchronized (files) {
                FilePages file = page.owner;
                if (file != null) {
                    if (page.previous == null) {
                        file.head = page.next;
                    } else {
                        page.previous.next = page.next;
                    }
                    if (page.next != null) {
                        page.next.previous = page.previous;
                    }
                    page.owner = null;
                    page.previous = null;
                    page.next = null;
                    if (file.head == null) {
                        files.remove(file.path, file);
                    }
                }
            }
        }
        value.callback.onRemoval(key);
    }

    @VisibleForTesting
    int cachedFileCount() {
        synchronized (files) {
            return files.size();
        }
    }

    private MemorySlice toMemorySlice(MemorySlice decoded) {
        if (!offHeap) {
            return decoded;
        }
        MemorySegment segment = MemorySegment.allocateOffHeapMemory(decoded.length());
        decoded.segment().copyTo(decoded.offset(), segment, 0, decoded.length());
        return MemorySlice.wrap(segment);
    }

    @Override
    public void close() {
        dataCache.invalidateAll();
        if (indexCache != dataCache) {
            indexCache.invalidateAll();
        }
    }

    private static class FilePages {
        private final Path path;
        private Page head;

        private FilePages(Path path) {
            this.path = path;
        }
    }

    /** Links are guarded by the file index lock; they are detached before notifying callers. */
    private static class Page extends CacheValue {
        private final CacheKey key;
        private FilePages owner;
        private Page previous;
        private Page next;

        private Page(CacheKey key, MemorySlice slice, CacheCallback callback) {
            super(slice, callback);
            this.key = key;
        }
    }
}
