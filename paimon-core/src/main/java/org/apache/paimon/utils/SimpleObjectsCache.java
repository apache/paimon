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
import org.apache.paimon.data.MultiSegments;
import org.apache.paimon.data.RandomAccessInputView;
import org.apache.paimon.data.Segments;
import org.apache.paimon.data.SimpleCollectingOutputView;
import org.apache.paimon.data.SingleSegments;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentSource;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Cache records to {@link SegmentsCache} by compacted serializer. */
@ThreadSafe
public class SimpleObjectsCache<K, V> extends ObjectsCache<K, V, Segments> {

    public SimpleObjectsCache(
            SegmentsCache<K> cache,
            ObjectSerializer<V> projectedSerializer,
            RowType formatSchema,
            FunctionWithIOException<K, Long> fileSizeFunction,
            BiFunctionWithIOE<K, Long, CloseableIterator<InternalRow>> reader) {
        super(cache, projectedSerializer, formatSchema, fileSizeFunction, reader);
    }

    public List<V> read(
            K key, @Nullable Long fileSize, Filter<InternalRow> readFilter, Filter<V> readVFilter)
            throws IOException {
        return read(key, fileSize, new Filters<>(readFilter, readVFilter));
    }

    @Override
    protected List<V> readFromSegments(Segments segments, Filters<V> filters) throws IOException {
        return readFromSegments(formatSerializer.get(), projectedSerializer, segments, filters);
    }

    @Override
    protected Segments createSegments(K key, @Nullable Long fileSize) {
        InternalRowSerializer formatSerializer = this.formatSerializer.get();
        try (CloseableIterator<InternalRow> iterator = reader.apply(key, fileSize)) {
            ArrayList<MemorySegment> segments = new ArrayList<>();
            MemorySegmentSource segmentSource =
                    () -> MemorySegment.allocateHeapMemory(cache.pageSize());
            SimpleCollectingOutputView output =
                    new SimpleCollectingOutputView(segments, segmentSource, cache.pageSize());
            while (iterator.hasNext()) {
                InternalRow row = iterator.next();
                formatSerializer.serializeToPages(row, output);
            }
            return Segments.create(segments, output.getCurrentPositionInSegment());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <V> List<V> readFromSegments(
            InternalRowSerializer formatSerializer,
            ObjectSerializer<V> projectedSerializer,
            Segments segments,
            Filters<V> filters)
            throws IOException {
        List<V> entries = new ArrayList<>();
        RandomAccessInputView view = createInputView(segments);
        BinaryRow binaryRow = new BinaryRow(formatSerializer.getArity());
        Filter<InternalRow> readFilter = filters.readFilter();
        Filter<V> readVFilter = filters.readVFilter();
        while (true) {
            try {
                formatSerializer.mapFromPages(binaryRow, view);
                if (readFilter.test(binaryRow)) {
                    V v = projectedSerializer.fromRow(binaryRow);
                    if (readVFilter.test(v)) {
                        entries.add(v);
                    }
                }
            } catch (EOFException e) {
                return entries;
            }
        }
    }

    private static RandomAccessInputView createInputView(Segments segments) {
        if (segments instanceof MultiSegments) {
            MultiSegments memorySegments = (MultiSegments) segments;
            return new RandomAccessInputView(
                    memorySegments.segments(),
                    memorySegments.pageSize(),
                    memorySegments.limitInLastSegment());
        } else if (segments instanceof SingleSegments) {
            SingleSegments singleSegments = (SingleSegments) segments;
            ArrayList<MemorySegment> array = new ArrayList<>();
            array.add(singleSegments.segment());
            return new RandomAccessInputView(
                    array, singleSegments.segment().size(), singleSegments.limit());
        } else {
            throw new IllegalArgumentException("Unsupported segment type: " + segments.getClass());
        }
    }
}
