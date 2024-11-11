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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.RandomAccessInputView;
import org.apache.paimon.data.Segments;
import org.apache.paimon.data.SimpleCollectingOutputView;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentSource;
import org.apache.paimon.types.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.ObjectsFile.readFromIterator;

/** Cache records to {@link SegmentsCache} by compacted serializer. */
@ThreadSafe
public class ObjectsCache<K, V> {
    protected static final Logger LOG = LoggerFactory.getLogger(ObjectsCache.class);

    private final SegmentsCache<K> cache;
    private final ObjectSerializer<V> projectedSerializer;
    private final ThreadLocal<InternalRowSerializer> formatSerializer;
    private final FunctionWithIOException<K, Long> fileSizeFunction;
    private final BiFunctionWithIOE<K, Long, CloseableIterator<InternalRow>> reader;

    public ObjectsCache(
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

    public List<V> read(
            K key,
            @Nullable Long fileSize,
            Filter<InternalRow> loadFilter,
            Filter<InternalRow> readFilter)
            throws IOException {
        Segments segments = cache.getIfPresents(key);
        if (segments != null) {
            return readFromSegments(segments, readFilter);
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("not match cache key {}", key);
            }
            if (fileSize == null) {
                fileSize = fileSizeFunction.apply(key);
            }
            if (fileSize <= cache.maxElementSize()) {
                segments = readSegments(key, fileSize, loadFilter);
                cache.put(key, segments);
                return readFromSegments(segments, readFilter);
            } else {
                return readFromIterator(
                        reader.apply(key, fileSize), projectedSerializer, readFilter);
            }
        }
    }

    private List<V> readFromSegments(Segments segments, Filter<InternalRow> readFilter)
            throws IOException {
        InternalRowSerializer formatSerializer = this.formatSerializer.get();
        List<V> entries = new ArrayList<>();
        RandomAccessInputView view =
                new RandomAccessInputView(
                        segments.segments(), cache.pageSize(), segments.limitInLastSegment());
        BinaryRow binaryRow = new BinaryRow(formatSerializer.getArity());
        while (true) {
            try {
                formatSerializer.mapFromPages(binaryRow, view);
                if (readFilter.test(binaryRow)) {
                    entries.add(projectedSerializer.fromRow(binaryRow));
                }
            } catch (EOFException e) {
                return entries;
            }
        }
    }

    private Segments readSegments(K key, @Nullable Long fileSize, Filter<InternalRow> loadFilter) {
        InternalRowSerializer formatSerializer = this.formatSerializer.get();
        try (CloseableIterator<InternalRow> iterator = reader.apply(key, fileSize)) {
            ArrayList<MemorySegment> segments = new ArrayList<>();
            MemorySegmentSource segmentSource =
                    () -> MemorySegment.allocateHeapMemory(cache.pageSize());
            SimpleCollectingOutputView output =
                    new SimpleCollectingOutputView(segments, segmentSource, cache.pageSize());
            while (iterator.hasNext()) {
                InternalRow row = iterator.next();
                if (loadFilter.test(row)) {
                    formatSerializer.serializeToPages(row, output);
                }
            }
            return new Segments(segments, output.getCurrentPositionInSegment());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
