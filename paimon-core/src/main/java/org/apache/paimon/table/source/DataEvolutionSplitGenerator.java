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
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.utils.BinPacking;
import org.apache.paimon.utils.RangeHelper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

import static java.util.Collections.reverseOrder;
import static java.util.Comparator.comparingLong;
import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
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
        return false;
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

    public static List<List<DataFileMeta>> split(List<DataFileMeta> files) {
        return split(
                files,
                DataFileMeta::fileName,
                DataFileMeta::firstRowId,
                DataFileMeta::rowCount,
                DataFileMeta::maxSequenceNumber);
    }

    public static List<List<ManifestEntry>> splitManifests(List<ManifestEntry> entries) {
        return split(
                entries,
                entry -> entry.file().fileName(),
                entry -> entry.file().firstRowId(),
                entry -> entry.file().rowCount(),
                entry -> entry.file().maxSequenceNumber());
    }

    public static <T> List<List<T>> split(
            List<T> files,
            Function<T, String> fileNameF,
            Function<T, Long> firstRowIdF,
            ToLongFunction<T> rowCountF,
            ToLongFunction<T> maxSeqF) {
        // group by row id range
        ToLongFunction<T> firstRowIdFunc =
                t -> {
                    Long firstRowId = firstRowIdF.apply(t);
                    checkArgument(
                            firstRowId != null, "File %s First row id should not be null.", t);
                    return firstRowId;
                };
        ToLongFunction<T> endRowIdF =
                t -> firstRowIdFunc.applyAsLong(t) + rowCountF.applyAsLong(t) - 1;
        RangeHelper<T> rangeHelper = new RangeHelper<>(firstRowIdFunc, endRowIdF);
        List<List<T>> result = rangeHelper.mergeOverlappingRanges(files);

        // skip group which only has blob files
        // TODO Why skip? When will this situation occur?
        result.removeIf(next -> next.stream().allMatch(t -> isBlobFile(fileNameF.apply(t))));

        // in group, sort by blob file and max_seq
        for (List<T> group : result) {
            // split to data files and blob files
            List<T> dataFiles = new ArrayList<>();
            List<T> blobFiles = new ArrayList<>();
            for (T t : group) {
                if (isBlobFile(fileNameF.apply(t))) {
                    blobFiles.add(t);
                } else {
                    dataFiles.add(t);
                }
            }

            // data files sort by reversed max sequence number
            dataFiles.sort(comparingLong(maxSeqF).reversed());
            checkArgument(
                    rangeHelper.areAllRangesSame(dataFiles),
                    "Data files %s should be all row id ranges same.",
                    dataFiles);

            // blob files sort by first row id then by reversed max sequence number
            // TODO Why is it sorted like this?
            blobFiles.sort(
                    comparingLong(firstRowIdFunc)
                            .thenComparing(reverseOrder(comparingLong(maxSeqF))));

            // concat data files and blob files
            group.clear();
            group.addAll(dataFiles);
            group.addAll(blobFiles);
        }

        return result;
    }
}
