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

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.utils.BinPacking;
import org.apache.paimon.utils.RangeHelper;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Append data evolution table split generator, which implementation of {@link SplitGenerator}. */
public class DataEvolutionSplitGenerator implements SplitGenerator {

    private final long targetSplitSize;
    private final long openFileCost;

    public DataEvolutionSplitGenerator(long targetSplitSize, long openFileCost) {
        this.targetSplitSize = targetSplitSize;
        this.openFileCost = openFileCost;
    }

    @Override
    public boolean alwaysRawConvertible() {
        return false;
    }

    @Override
    public List<SplitGroup> splitForBatch(List<DataFileMeta> input) {
        RangeHelper<DataFileMeta> rangeHelper =
                new RangeHelper<>(
                        DataFileMeta::nonNullFirstRowId,
                        f -> f.nonNullFirstRowId() + f.rowCount() - 1);
        List<List<DataFileMeta>> ranges = rangeHelper.mergeOverlappingRanges(input);
        Function<List<DataFileMeta>, Long> weightFunc =
                file ->
                        Math.max(
                                file.stream().mapToLong(DataFileMeta::fileSize).sum(),
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
