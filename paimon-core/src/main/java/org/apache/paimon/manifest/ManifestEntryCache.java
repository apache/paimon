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
import org.apache.paimon.io.DataPagedOutputSerializer;
import org.apache.paimon.manifest.ManifestEntrySegments.RichSegments;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.partition.PartitionPredicate.MultiplePartitionPredicate;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BiFunctionWithIOE;
import org.apache.paimon.utils.CloseableIterator;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;

import static org.apache.paimon.manifest.ManifestEntrySerializer.bucketGetter;
import static org.apache.paimon.manifest.ManifestEntrySerializer.partitionGetter;
import static org.apache.paimon.manifest.ManifestEntrySerializer.totalBucketGetter;

/**
 * Cache {@link ManifestEntry} records to {@link SegmentsCache}, unlike {@link SimpleObjectsCache},
 * it builds fast indexed queries based on {@link ManifestEntryFilters} and {@link
 * ManifestEntrySegments}.
 */
@ThreadSafe
public class ManifestEntryCache
        extends ObjectsCache<BlockKey, ManifestEntry, ManifestEntrySegments> {

    @Nullable private final FilteredReader filteredReader;

    public ManifestEntryCache(
            SegmentsCache<BlockKey> cache,
            ObjectSerializer<ManifestEntry> projectedSerializer,
            RowType formatSchema,
            FunctionWithIOException<BlockKey, Long> fileSizeFunction,
            BiFunctionWithIOE<BlockKey, Long, CloseableIterator<InternalRow>> reader) {
        this(cache, projectedSerializer, formatSchema, fileSizeFunction, reader, null);
    }

    public ManifestEntryCache(
            SegmentsCache<BlockKey> cache,
            ObjectSerializer<ManifestEntry> projectedSerializer,
            RowType formatSchema,
            FunctionWithIOException<BlockKey, Long> fileSizeFunction,
            BiFunctionWithIOE<BlockKey, Long, CloseableIterator<InternalRow>> reader,
            @Nullable FilteredReader filteredReader) {
        super(cache, projectedSerializer, formatSchema, fileSizeFunction, reader);
        this.filteredReader = filteredReader;
    }

    /** Uncached reads skip non-matching partitions and buckets before decoding their stats. */
    @Override
    protected CloseableIterator<InternalRow> createFilteredIterator(
            BlockKey key, @Nullable Long fileSize, Filters<ManifestEntry> filters)
            throws IOException {
        if (filteredReader != null && filters instanceof ManifestEntryFilters) {
            ManifestEntryFilters manifestFilters = (ManifestEntryFilters) filters;
            return filteredReader.read(
                    key, fileSize, manifestFilters.partitionFilter, manifestFilters.bucketFilter);
        }
        return super.createFilteredIterator(key, fileSize, filters);
    }

    /** Reader of manifest rows which can skip entries by partition and bucket while decoding. */
    @FunctionalInterface
    public interface FilteredReader {
        CloseableIterator<InternalRow> read(
                BlockKey key,
                @Nullable Long fileSize,
                @Nullable PartitionPredicate partitionFilter,
                @Nullable BucketFilter bucketFilter)
                throws IOException;
    }

    @Override
    protected ManifestEntrySegments createSegments(BlockKey key, @Nullable Long fileSize) {
        List<RichSegments> segments = new ArrayList<>();
        Function<InternalRow, BinaryRow> partitionGetter = partitionGetter();
        Function<InternalRow, Integer> bucketGetter = bucketGetter();
        Function<InternalRow, Integer> totalBucketGetter = totalBucketGetter();
        int pageSize = cache.pageSize();
        InternalRowSerializer formatSerializer = this.formatSerializer.get();
        Triple<BinaryRow, Integer, Integer> group = null;
        DataPagedOutputSerializer output = null;
        long completedBytes = 0;
        long limit = Math.min(cache.maxElementSize(), cache.maxMemorySize().getBytes());
        try (CloseableIterator<InternalRow> iterator = reader.apply(key, fileSize)) {
            while (iterator.hasNext()) {
                InternalRow row = iterator.next();
                BinaryRow partition = partitionGetter.apply(row);
                int bucket = bucketGetter.apply(row);
                int totalBucket = totalBucketGetter.apply(row);
                // Keep consecutive runs rather than regrouping the block: physical entry order
                // must survive cache hits, including ADD/DELETE entries and overlapping row IDs.
                if (group == null
                        || !group.f0.equals(partition)
                        || group.f1 != bucket
                        || group.f2 != totalBucket) {
                    if (output != null) {
                        RichSegments completed = finish(group, output);
                        segments.add(completed);
                        completedBytes += completed.totalMemorySize();
                    }
                    group = Triple.of(partition.copy(), bucket, totalBucket);
                    output = new DataPagedOutputSerializer(formatSerializer, 2048, pageSize);
                }
                output.write(row);
                if (completedBytes + output.memorySize() + RichSegments.metadataMemorySize(group.f0)
                        > limit) {
                    throw new CacheLimitExceeded();
                }
            }
            if (output != null) {
                segments.add(finish(group, output));
            }
            return new ManifestEntrySegments(segments);
        } catch (CacheLimitExceeded e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static RichSegments finish(
            Triple<BinaryRow, Integer, Integer> group, DataPagedOutputSerializer output)
            throws IOException {
        SimpleCollectingOutputView view = output.close();
        Segments data = Segments.create(view.fullSegments(), view.getCurrentPositionInSegment());
        return new RichSegments(group.f0, group.f1, group.f2, data);
    }

    <R> List<R> readCached(
            ManifestEntrySegments entries,
            Filters<ManifestEntry> filters,
            Function<ManifestEntry, R> convertor)
            throws IOException {
        if (cacheMetrics != null) {
            cacheMetrics.increaseHitObject();
        }
        return readFromSegments(entries, filters, convertor);
    }

    @Override
    public <R> List<R> read(
            BlockKey key,
            @Nullable Long fileSize,
            Filters<ManifestEntry> filters,
            Function<ManifestEntry, R> convertor)
            throws IOException {
        try {
            return super.read(key, fileSize, filters, convertor);
        } catch (CacheLimitExceeded ignored) {
            // The query-local reader can replay its current raw block without another file read.
            // Do not cache a filtered prefix of a block that exceeded the decoded-memory limit.
            return org.apache.paimon.utils.ObjectsFile.readFromIterator(
                    createFilteredIterator(key, fileSize, filters),
                    projectedSerializer,
                    filters.readFilter(),
                    filters.readVFilter(),
                    convertor);
        }
    }

    private static final class CacheLimitExceeded extends RuntimeException {

        private static final long serialVersionUID = 1L;

        private CacheLimitExceeded() {
            super(null, null, false, false);
        }
    }

    @Override
    protected <R> List<R> readFromSegments(
            ManifestEntrySegments manifestSegments,
            Filters<ManifestEntry> filters,
            Function<ManifestEntry, R> convertor)
            throws IOException {
        PartitionPredicate partitionFilter = null;
        BucketFilter bucketFilter = null;
        if (filters instanceof ManifestEntryFilters) {
            partitionFilter = ((ManifestEntryFilters) filters).partitionFilter;
            bucketFilter = ((ManifestEntryFilters) filters).bucketFilter;
        }

        List<RichSegments> segments = manifestSegments.segments();

        // try to do fast filter first
        Optional<BinaryRow> singlePartition = extractSinglePartition(partitionFilter);
        if (singlePartition.isPresent()) {
            Map<Integer, List<RichSegments>> segMap =
                    manifestSegments.indexedSegments().get(singlePartition.get());
            if (segMap == null) {
                return Collections.emptyList();
            }
            OptionalInt specifiedBucket = extractSpecifiedBucket(bucketFilter);
            if (specifiedBucket.isPresent()) {
                segments = segMap.get(specifiedBucket.getAsInt());
                if (segments == null) {
                    return Collections.emptyList();
                }
            }
        }

        // do force loop filter
        List<Segments> segmentsList = new ArrayList<>();
        for (RichSegments richSegments : segments) {
            BinaryRow partition = richSegments.partition();
            if (partitionFilter != null && !partitionFilter.test(partition)) {
                continue;
            }
            if (bucketFilter != null
                    && !bucketFilter.test(
                            partition, richSegments.bucket(), richSegments.totalBucket())) {
                continue;
            }
            segmentsList.add(richSegments.segments());
        }

        // read manifest entries from segments with per record filter
        List<R> result = new ArrayList<>();
        InternalRowSerializer formatSerializer = this.formatSerializer.get();
        for (Segments subSegments : segmentsList) {
            result.addAll(
                    SimpleObjectsCache.readFromSegments(
                            formatSerializer,
                            projectedSerializer,
                            subSegments,
                            filters,
                            convertor));
        }
        return result;
    }

    private Optional<BinaryRow> extractSinglePartition(@Nullable PartitionPredicate predicate) {
        if (predicate instanceof MultiplePartitionPredicate) {
            return ((MultiplePartitionPredicate) predicate).extractSinglePartition();
        }
        return Optional.empty();
    }

    private OptionalInt extractSpecifiedBucket(@Nullable BucketFilter filter) {
        if (filter != null) {
            Integer specifiedBucket = filter.specifiedBucket();
            if (specifiedBucket != null) {
                return OptionalInt.of(specifiedBucket);
            }
        }
        return OptionalInt.empty();
    }
}
