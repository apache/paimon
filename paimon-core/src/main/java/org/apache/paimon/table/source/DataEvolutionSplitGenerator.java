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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

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
        return true;
    }

    @Override
    public List<SplitGroup> splitForBatch(List<DataFileMeta> input) {
        List<List<DataFileMeta>> files = split(input);
        Function<List<DataFileMeta>, Long> weightFunc =
                file ->
                        Math.max(
                                file.stream().mapToLong(DataFileMeta::fileSize).sum(),
                                openFileCost);
        return BinPacking.packForOrdered(files, weightFunc, targetSplitSize).stream()
                .flatMap(Collection::stream)
                .map(SplitGroup::rawConvertibleGroup)
                .collect(Collectors.toList());
    }

    @Override
    public List<SplitGroup> splitForStreaming(List<DataFileMeta> files) {
        return splitForBatch(files);
    }

    public static List<List<DataFileMeta>> split(List<DataFileMeta> files) {
        List<List<DataFileMeta>> splitByRowId = new ArrayList<>();
        // Sort files by firstRowId and then by maxSequenceNumber
        files.sort(
                Comparator.comparingLong(
                                (ToLongFunction<DataFileMeta>)
                                        value ->
                                                value.firstRowId() == null
                                                        ? Long.MIN_VALUE
                                                        : value.firstRowId())
                        .thenComparing(
                                (f1, f2) -> {
                                    // If firstRowId is the same, we should read the file with
                                    // larger sequence number first. Because larger sequence number
                                    // file is more fresh
                                    return Long.compare(
                                            f2.maxSequenceNumber(), f1.maxSequenceNumber());
                                }));

        // Split files by firstRowId
        long lastRowId = -1;
        long checkRowIdStart = 0;
        List<DataFileMeta> currentSplit = new ArrayList<>();
        for (DataFileMeta file : files) {
            Long firstRowId = file.firstRowId();
            if (firstRowId == null) {
                splitByRowId.add(Collections.singletonList(file));
                continue;
            }
            if (firstRowId != lastRowId) {
                if (!currentSplit.isEmpty()) {
                    splitByRowId.add(currentSplit);
                }
                checkArgument(
                        firstRowId >= checkRowIdStart,
                        "There are overlapping files in the split: \n %s, the wrong file is: \n %s",
                        files.stream().map(DataFileMeta::toString).collect(Collectors.joining(",")),
                        file);
                currentSplit = new ArrayList<>();
                lastRowId = firstRowId;
                checkRowIdStart = firstRowId + file.rowCount();
            }
            currentSplit.add(file);
        }
        if (!currentSplit.isEmpty()) {
            splitByRowId.add(currentSplit);
        }

        return splitByRowId;
    }
}
