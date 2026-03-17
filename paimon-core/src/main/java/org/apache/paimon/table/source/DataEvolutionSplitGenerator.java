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
    private final boolean forceSplitRowRangeContigous;

    public DataEvolutionSplitGenerator(
            long targetSplitSize,
            long openFileCost,
            boolean countBlobSize,
            boolean forceSplitRowRangeContigous) {
        this.targetSplitSize = targetSplitSize;
        this.openFileCost = openFileCost;
        this.countBlobSize = countBlobSize;
        this.forceSplitRowRangeContigous = forceSplitRowRangeContigous;
    }

    @Override
    public boolean alwaysRawConvertible() {
        return false;
    }

    @Override
    public List<SplitGroup> splitForBatch(List<DataFileMeta> input) {
        RangeHelper<DataFileMeta> rangeHelper = new RangeHelper<>(DataFileMeta::nonNullRowIdRange);
        List<List<DataFileMeta>> ranges = rangeHelper.mergeOverlappingRanges(input);
        Function<List<DataFileMeta>, Long> weightFunc =
                file ->
                        Math.max(
                                file.stream()
                                        .mapToLong(
                                                meta ->
                                                        BlobFileFormat.isBlobFile(meta.fileName())
                                                                ? countBlobSize
                                                                        ? meta.fileSize()
                                                                        : openFileCost
                                                                : meta.fileSize())
                                        .sum(),
                                openFileCost);
        if (forceSplitRowRangeContigous) {
            return packByContiguousRanges(ranges, weightFunc);
        }
        return BinPacking.packForOrdered(ranges, weightFunc, targetSplitSize).stream()
                .map(this::toPackedSplitGroup)
                .collect(Collectors.toList());
    }

    @Override
    public List<SplitGroup> splitForStreaming(List<DataFileMeta> files) {
        return splitForBatch(files);
    }

    private SplitGroup toPackedSplitGroup(List<List<DataFileMeta>> fileGroups) {
        boolean rawConvertible = fileGroups.stream().allMatch(file -> file.size() == 1);
        List<DataFileMeta> groupFiles =
                fileGroups.stream().flatMap(Collection::stream).collect(Collectors.toList());
        return rawConvertible
                ? SplitGroup.rawConvertibleGroup(groupFiles)
                : SplitGroup.nonRawConvertibleGroup(groupFiles);
    }

    private List<SplitGroup> packByContiguousRanges(
            List<List<DataFileMeta>> ranges, Function<List<DataFileMeta>, Long> weightFunc) {
        if (ranges.isEmpty()) {
            return new ArrayList<>();
        }

        List<SplitGroup> result = new ArrayList<>();
        List<List<DataFileMeta>> currentSegment = new ArrayList<>();
        long currentMaxRowId = Long.MIN_VALUE;

        for (List<DataFileMeta> rangeFiles : ranges) {
            long minRowId = minRowId(rangeFiles);
            long maxRowId = maxRowId(rangeFiles);
            if (currentSegment.isEmpty() || areContiguous(currentMaxRowId, minRowId)) {
                currentSegment.add(rangeFiles);
                currentMaxRowId = maxRowId;
            } else {
                result.addAll(
                        BinPacking.packForOrdered(currentSegment, weightFunc, targetSplitSize)
                                .stream()
                                .map(this::toPackedSplitGroup)
                                .collect(Collectors.toList()));
                currentSegment = new ArrayList<>();
                currentSegment.add(rangeFiles);
                currentMaxRowId = maxRowId;
            }
        }

        result.addAll(
                BinPacking.packForOrdered(currentSegment, weightFunc, targetSplitSize).stream()
                        .map(this::toPackedSplitGroup)
                        .collect(Collectors.toList()));
        return result;
    }

    private long minRowId(List<DataFileMeta> files) {
        return files.stream()
                .mapToLong(f -> f.nonNullRowIdRange().from)
                .min()
                .orElse(Long.MAX_VALUE);
    }

    private long maxRowId(List<DataFileMeta> files) {
        return files.stream().mapToLong(f -> f.nonNullRowIdRange().to).max().orElse(Long.MIN_VALUE);
    }

    private boolean areContiguous(long previousMaxRowId, long currentMinRowId) {
        // Contiguous means no gap between adjacent ranges.
        // e.g. previous max == current min (as requested) or previous max + 1 == current min.
        return previousMaxRowId >= currentMinRowId - 1;
    }
}
