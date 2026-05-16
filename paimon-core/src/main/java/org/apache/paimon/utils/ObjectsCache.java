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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Segments;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.operation.metrics.CacheMetrics;
import org.apache.paimon.types.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.List;

import static org.apache.paimon.utils.ObjectsFile.readFromIterator;

/** Cache records to {@link SegmentsCache} by compacted serializer. */
@ThreadSafe
public abstract class ObjectsCache<K, V, S extends Segments> {

    private static final Logger LOG = LoggerFactory.getLogger(ObjectsCache.class);

    protected final SegmentsCache<K> cache;
    protected final ObjectSerializer<V> projectedSerializer;
    protected final ThreadLocal<InternalRowSerializer> formatSerializer;
    protected final FunctionWithIOException<K, Long> fileSizeFunction;
    protected final BiFunctionWithIOE<K, Long, CloseableIterator<InternalRow>> reader;

    @Nullable protected CacheMetrics cacheMetrics;

    protected ObjectsCache(
            SegmentsCache<K> cache,
            ObjectSerializer<V> projectedSerializer,
            RowType formatSchema,
            FunctionWithIOException<K, Long> fileSizeFunction,
            BiFunctionWithIOE<K, Long, CloseableIterator<InternalRow>> reader) {
        this.cache = cache;
        this.projectedSerializer = projectedSerializer;
        this.formatSerializer =
                ThreadLocal.withInitial(() -> new InternalRowSerializer(formatSchema));
        this.fileSizeFunction = fileSizeFunction;
        this.reader = reader;
    }

    public void withCacheMetrics(@Nullable CacheMetrics cacheMetrics) {
        this.cacheMetrics = cacheMetrics;
    }

    public List<V> read(K key, @Nullable Long fileSize, Filters<V> filters) throws IOException {
        @SuppressWarnings("unchecked")
        S segments = (S) cache.getIfPresents(key);
        if (segments != null) {
            if (cacheMetrics != null) {
                cacheMetrics.increaseHitObject();
            }
            LOG.info("ObjectsCache cache-hit for {}", key.toString());
            return readFromSegments(segments, filters);
        } else {
            LOG.info("ObjectsCache cache-miss for {}", key.toString());
            if (cacheMetrics != null) {
                cacheMetrics.increaseMissedObject();
            }
            if (fileSize == null) {
                fileSize = fileSizeFunction.apply(key);
            }
            if (fileSize <= cache.maxElementSize()) {
                segments = createSegments(key, fileSize);
                cache.put(key, segments);
                return readFromSegments(segments, filters);
            } else {
                return readFromIterator(
                        reader.apply(key, fileSize),
                        projectedSerializer,
                        filters.readFilter(),
                        filters.readVFilter());
            }
        }
    }

    protected abstract List<V> readFromSegments(S segments, Filters<V> filters) throws IOException;

    protected abstract S createSegments(K k, @Nullable Long fileSize);

    /** Filter context for reading. */
    public static class Filters<V> {

        private final Filter<InternalRow> readFilter;
        private final Filter<V> readVFilter;

        public Filters(Filter<InternalRow> readFilter, Filter<V> readVFilter) {
            this.readFilter = readFilter;
            this.readVFilter = readVFilter;
        }

        public Filter<InternalRow> readFilter() {
            return readFilter;
        }

        public Filter<V> readVFilter() {
            return readVFilter;
        }
    }
}
