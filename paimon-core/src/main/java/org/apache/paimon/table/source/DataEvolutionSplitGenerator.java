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
import org.apache.paimon.utils.DataEvolutionUtils;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.RangeHelper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
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
        // A file without a first row id predates row tracking on this table (see
        // DataEvolutionUtils#splitByRowIdPresence): it holds complete rows and is split like a
        // plain append file, after the row-id-range groups.
        Pair<List<DataFileMeta>, List<DataFileMeta>> byRowId =
                DataEvolutionUtils.splitByRowIdPresence(input, file -> file);
        List<SplitGroup> groups = new ArrayList<>(splitRowIdRanges(byRowId.getLeft()));
        List<DataFileMeta> withoutRowId = new ArrayList<>(byRowId.getRight());
        withoutRowId.sort(Comparator.comparing(DataFileMeta::minSequenceNumber));
        BinPacking.packForOrdered(
                        withoutRowId,
                        file -> Math.max(file.fileSize(), openFileCost),
                        targetSplitSize)
                .forEach(files -> groups.add(SplitGroup.rawConvertibleGroup(files)));
        return groups;
    }

    private List<SplitGroup> splitRowIdRanges(List<DataFileMeta> input) {
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
}
