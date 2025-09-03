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

package org.apache.paimon.manifest;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Segments;
import org.apache.paimon.data.SimpleCollectingOutputView;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestSegments.RichSegments;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentSource;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BiFunctionWithIOE;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.FunctionWithIOException;
import org.apache.paimon.utils.ObjectSerializer;
import org.apache.paimon.utils.ObjectsCache;
import org.apache.paimon.utils.SegmentsCache;
import org.apache.paimon.utils.SimpleObjectsCache;
import org.apache.paimon.utils.Triple;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.manifest.ManifestEntrySerializer.bucketGetter;
import static org.apache.paimon.manifest.ManifestEntrySerializer.partitionGetter;
import static org.apache.paimon.manifest.ManifestEntrySerializer.totalBucketGetter;

/** Cache records to {@link SegmentsCache} by compacted serializer. */
@ThreadSafe
public class ManifestObjectsCache
        extends ObjectsCache<Path, ManifestEntry, ManifestEntryFilters, ManifestSegments> {

    public ManifestObjectsCache(
            SegmentsCache<Path> cache,
            ObjectSerializer<ManifestEntry> projectedSerializer,
            RowType formatSchema,
            FunctionWithIOException<Path, Long> fileSizeFunction,
            BiFunctionWithIOE<Path, Long, CloseableIterator<InternalRow>> reader) {
        super(cache, projectedSerializer, formatSchema, fileSizeFunction, reader);
    }

    @Override
    protected ManifestSegments createSegments(Path path, @Nullable Long fileSize) {
        Map<Triple<BinaryRow, Integer, Integer>, SimpleCollectingOutputView> segments =
                new HashMap<>();
        Function<InternalRow, BinaryRow> partitionGetter = partitionGetter();
        Function<InternalRow, Integer> bucketGetter = bucketGetter();
        Function<InternalRow, Integer> totalBucketGetter = totalBucketGetter();
        MemorySegmentSource segmentSource =
                () -> MemorySegment.allocateHeapMemory(cache.pageSize());
        InternalRowSerializer formatSerializer = this.formatSerializer.get();
        try (CloseableIterator<InternalRow> iterator = reader.apply(path, fileSize)) {
            while (iterator.hasNext()) {
                InternalRow row = iterator.next();
                BinaryRow partition = partitionGetter.apply(row);
                int bucket = bucketGetter.apply(row);
                int totalBucket = totalBucketGetter.apply(row);
                Triple<BinaryRow, Integer, Integer> key = Triple.of(partition, bucket, totalBucket);
                SimpleCollectingOutputView view =
                        segments.computeIfAbsent(
                                key,
                                k ->
                                        new SimpleCollectingOutputView(
                                                new ArrayList<>(),
                                                segmentSource,
                                                cache.pageSize()));
                formatSerializer.serializeToPages(row, view);
            }
            List<RichSegments> result = new ArrayList<>();
            segments.forEach(
                    (k, v) ->
                            result.add(
                                    new RichSegments(
                                            k.f0,
                                            k.f1,
                                            k.f2,
                                            Segments.create(
                                                    v.fullSegments(),
                                                    v.getCurrentPositionInSegment()))));
            return new ManifestSegments(result);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected List<ManifestEntry> readFromSegments(
            ManifestSegments manifestSegments, ManifestEntryFilters filters) throws IOException {
        List<Segments> segmentsList = new ArrayList<>();
        PartitionPredicate partitionFilter = filters.partitionFilter;
        BucketFilter bucketFilter = filters.bucketFilter;
        List<RichSegments> segments = manifestSegments.segments();
        if (partitionFilter != null) {
            // try to do fast filter first
            Optional<BinaryRow> partition = partitionFilter.extractSinglePartition();
            if (partition.isPresent()) {
                Map<Integer, List<RichSegments>> segMap =
                        manifestSegments.indexedSegments().get(partition.get());
                if (bucketFilter != null && bucketFilter.specifiedBucket() != null) {
                    segments = segMap.get(bucketFilter.specifiedBucket());
                } else {
                    segments =
                            segMap.values().stream()
                                    .flatMap(List::stream)
                                    .collect(Collectors.toList());
                }
            }
        }
        segments.forEach(
                (k, v) -> {
                    if (partitionFilter != null && !partitionFilter.test(k.f0)) {
                        return;
                    }
                    if (bucketFilter != null && !bucketFilter.test(k.f1, k.f2)) {
                        return;
                    }
                    segmentsList.add(v);
                });
        List<ManifestEntry> result = new ArrayList<>();
        InternalRowSerializer formatSerializer = this.formatSerializer.get();
        for (Segments subSegments : segmentsList) {
            result.addAll(
                    SimpleObjectsCache.readFromSegments(
                            formatSerializer, projectedSerializer, subSegments, filters));
        }
        return result;
    }

    private Filter<BinaryRow> createPartitionFilter(PartitionPredicate partitionFilter) {}
}
