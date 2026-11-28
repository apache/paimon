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

package org.apache.paimon.table.source;

import org.apache.paimon.format.blob.BlobFileFormat;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.utils.BinPacking;
import org.apache.paimon.utils.RangeHelper;
import org.apache.paimon.utils.ThrowingConsumer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Append data evolution table split generator, which implementation of {@link SplitGenerator}. */
public class DataEvolutionSplitGenerator implements SplitGenerator {

    private final long targetSplitSize;
    private final long openFileCost;
    private final boolean countBlobSize;

    public DataEvolutionSplitGenerator(
            long targetSplitSize, long openFileCost, boolean countBlobSize) {
        this.targetSplitSize = targetSplitSize;
        this.openFileCost = openFileCost;
        this.countBlobSize = countBlobSize;
    }

    @Override
    public boolean alwaysRawConvertible() {
        return false;
    }

    @Override
    public List<SplitGroup> splitForBatch(List<DataFileMeta> input) {
        RangeHelper<DataFileMeta> rangeHelper = new RangeHelper<>(DataFileMeta::nonNullRowIdRange);
        List<List<DataFileMeta>> ranges = rangeHelper.mergeOverlappingRanges(input);
        Function<List<DataFileMeta>, Long> weightFunc = this::rangeWeight;
        return BinPacking.packForOrdered(ranges, weightFunc, targetSplitSize).stream()
                .map(
                        f -> {
                            boolean rawConvertible = f.stream().allMatch(file -> file.size() == 1);
                            List<DataFileMeta> groupFiles =
                                    f.stream()
                                            .flatMap(Collection::stream)
                                            .collect(Collectors.toList());
                            return rawConvertible
                                    ? SplitGroup.rawConvertibleGroup(groupFiles)
                                    : SplitGroup.nonRawConvertibleGroup(groupFiles);
                        })
                .collect(Collectors.toList());
    }

    @Override
    public List<SplitGroup> splitForStreaming(List<DataFileMeta> files) {
        return splitForBatch(files);
    }

    /** Packs complete, ordered RowID groups without keeping all groups until planning finishes. */
    public <E extends Exception> BatchPacker<E> newBatchPacker(
            int maxPendingFiles, ThrowingConsumer<SplitGroup, E> consumer) {
        return new BatchPacker<>(maxPendingFiles, consumer);
    }

    private long rangeWeight(List<DataFileMeta> files) {
        return Math.max(
                files.stream()
                        .mapToLong(
                                meta ->
                                        BlobFileFormat.isBlobFile(meta.fileName())
                                                ? countBlobSize ? meta.fileSize() : openFileCost
                                                : meta.fileSize())
                        .sum(),
                openFileCost);
    }

    /** The file cap applies between groups; a larger connected group is never cut apart. */
    public final class BatchPacker<E extends Exception> {
        private final int maxPendingFiles;
        private final ThrowingConsumer<SplitGroup, E> consumer;
        private List<DataFileMeta> files = new ArrayList<>();
        private long weight;
        private boolean rawConvertible = true;
        private boolean finished;
        private boolean failed;

        private BatchPacker(int maxPendingFiles, ThrowingConsumer<SplitGroup, E> consumer) {
            if (maxPendingFiles <= 0) {
                throw new IllegalArgumentException("Pending file limit must be positive.");
            }
            this.maxPendingFiles = maxPendingFiles;
            this.consumer = consumer;
        }

        public void addRangeGroup(List<DataFileMeta> group) throws E {
            checkFailure();
            if (finished) {
                throw new IllegalStateException("Split packing has finished.");
            }
            if (group.isEmpty()) {
                throw new IllegalArgumentException("A RowID group must not be empty.");
            }
            long groupWeight = rangeWeight(group);
            if (!files.isEmpty()
                    && (weight + groupWeight > targetSplitSize
                            || (long) files.size() + group.size() > maxPendingFiles)) {
                flush();
            }
            files.addAll(group);
            weight += groupWeight;
            rawConvertible &= group.size() == 1;
        }

        public void finish() throws E {
            checkFailure();
            if (!finished) {
                finished = true;
                flush();
            }
        }

        private void flush() throws E {
            if (files.isEmpty()) {
                return;
            }
            SplitGroup completed =
                    rawConvertible
                            ? SplitGroup.rawConvertibleGroup(files)
                            : SplitGroup.nonRawConvertibleGroup(files);
            files = new ArrayList<>();
            weight = 0;
            rawConvertible = true;
            boolean success = false;
            try {
                consumer.accept(completed);
                success = true;
            } finally {
                failed = !success;
            }
        }

        private void checkFailure() {
            if (failed) {
                throw new IllegalStateException("Split packing failed; discard this plan.");
            }
        }
    }
}
