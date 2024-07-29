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

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

/** Cache records to {@link SegmentsCache} by compacted serializer. */
@ThreadSafe
public class ObjectsCache<K, V> {

    private final SegmentsCache<K> cache;
    private final ObjectSerializer<V> serializer;
    private final BiFunction<K, Long, CloseableIterator<InternalRow>> reader;
    private final ThreadLocal<InternalRowSerializer> threadLocalRowSerializer;

    public ObjectsCache(
            SegmentsCache<K> cache,
            ObjectSerializer<V> serializer,
            BiFunction<K, Long, CloseableIterator<InternalRow>> reader) {
        this.cache = cache;
        this.serializer = serializer;
        this.threadLocalRowSerializer =
                ThreadLocal.withInitial(() -> new InternalRowSerializer(serializer.fieldTypes()));
        this.reader = reader;
    }

    public List<V> read(
            K key,
            @Nullable Long fileSize,
            Filter<InternalRow> loadFilter,
            Filter<InternalRow> readFilter)
            throws IOException {
        InternalRowSerializer rowSerializer = threadLocalRowSerializer.get();
        Segments segments =
                cache.getSegments(key, k -> readSegments(k, fileSize, loadFilter, rowSerializer));
        List<V> entries = new ArrayList<>();
        RandomAccessInputView view =
                new RandomAccessInputView(
                        segments.segments(), cache.pageSize(), segments.limitInLastSegment());
        BinaryRow binaryRow = new BinaryRow(rowSerializer.getArity());
        while (true) {
            try {
                rowSerializer.mapFromPages(binaryRow, view);
                if (readFilter.test(binaryRow)) {
                    entries.add(serializer.fromRow(binaryRow));
                }
            } catch (EOFException e) {
                return entries;
            }
        }
    }

    private Segments readSegments(
            K key,
            @Nullable Long fileSize,
            Filter<InternalRow> loadFilter,
            InternalRowSerializer rowSerializer) {
        try (CloseableIterator<InternalRow> iterator = reader.apply(key, fileSize)) {
            ArrayList<MemorySegment> segments = new ArrayList<>();
            MemorySegmentSource segmentSource =
                    () -> MemorySegment.allocateHeapMemory(cache.pageSize());
            SimpleCollectingOutputView output =
                    new SimpleCollectingOutputView(segments, segmentSource, cache.pageSize());
            while (iterator.hasNext()) {
                InternalRow row = iterator.next();
                if (loadFilter.test(row)) {
                    rowSerializer.serializeToPages(row, output);
                }
            }
            return new Segments(segments, output.getCurrentPositionInSegment());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
