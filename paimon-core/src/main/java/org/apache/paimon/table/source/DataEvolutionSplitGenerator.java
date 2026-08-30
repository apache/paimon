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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.types.VectorType.isVectorStoreFile;
import static org.apache.paimon.utils.DataEvolutionUtils.groupByNormalFileRange;

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
        List<List<DataFileMeta>> ranges = groupByNormalFileRange(input, Function.identity());
        Map<DataFileMeta, Integer> sidecarOccurrences = new IdentityHashMap<>();
        for (List<DataFileMeta> range : ranges) {
            Set<DataFileMeta> rangeSidecars =
                    range.stream()
                            .filter(DataEvolutionSplitGenerator::isSidecar)
                            .collect(
                                    Collectors.toCollection(
                                            () ->
                                                    Collections.newSetFromMap(
                                                            new IdentityHashMap<>())));
            rangeSidecars.forEach(
                    file ->
                            sidecarOccurrences.put(
                                    file, sidecarOccurrences.getOrDefault(file, 0) + 1));
        }
        Set<DataFileMeta> sharedSidecars = Collections.newSetFromMap(new IdentityHashMap<>());
        sidecarOccurrences.forEach(
                (file, occurrences) -> {
                    if (occurrences > 1) {
                        sharedSidecars.add(file);
                    }
                });
        boolean hasSpanningSidecar = !sharedSidecars.isEmpty();
        List<List<List<DataFileMeta>>> packed =
                hasSpanningSidecar
                        ? packWithUniqueSidecars(ranges, sharedSidecars)
                        : BinPacking.packForOrdered(ranges, this::rangeWeight, targetSplitSize);
        return packed.stream()
                .map(
                        f -> {
                            boolean rawConvertible = f.stream().allMatch(file -> file.size() == 1);
                            Set<DataFileMeta> unique =
                                    Collections.newSetFromMap(new IdentityHashMap<>());
                            List<DataFileMeta> groupFiles =
                                    f.stream()
                                            .flatMap(Collection::stream)
                                            .filter(unique::add)
                                            .collect(Collectors.toList());
                            return rawConvertible
                                    ? SplitGroup.rawConvertibleGroup(groupFiles)
                                    : SplitGroup.nonRawConvertibleGroup(groupFiles);
                        })
                .collect(Collectors.toList());
    }

    private List<List<List<DataFileMeta>>> packWithUniqueSidecars(
            List<List<DataFileMeta>> ranges, Set<DataFileMeta> sharedSidecars) {
        List<List<List<DataFileMeta>>> packed = new ArrayList<>();
        List<List<DataFileMeta>> current = new ArrayList<>();
        Set<DataFileMeta> seenSidecars = Collections.newSetFromMap(new IdentityHashMap<>());
        long currentWeight = 0;

        for (List<DataFileMeta> range : ranges) {
            long weight = incrementalRangeWeight(range, seenSidecars, sharedSidecars);
            if (!current.isEmpty() && currentWeight + weight > targetSplitSize) {
                packed.add(current);
                current = new ArrayList<>();
                seenSidecars = Collections.newSetFromMap(new IdentityHashMap<>());
                currentWeight = 0;
                weight = incrementalRangeWeight(range, seenSidecars, sharedSidecars);
            }
            current.add(range);
            currentWeight += weight;
            range.stream()
                    .filter(DataEvolutionSplitGenerator::isSidecar)
                    .forEach(seenSidecars::add);
        }

        if (!current.isEmpty()) {
            packed.add(current);
        }
        return packed;
    }

    private long incrementalRangeWeight(
            List<DataFileMeta> files,
            Set<DataFileMeta> seenSidecars,
            Set<DataFileMeta> sharedSidecars) {
        Set<DataFileMeta> seenInRange = Collections.newSetFromMap(new IdentityHashMap<>());
        long size =
                files.stream()
                        .filter(
                                file ->
                                        !isSidecar(file)
                                                || (!sharedSidecars.contains(file)
                                                        && !seenSidecars.contains(file)
                                                        && seenInRange.add(file)))
                        .mapToLong(this::fileWeight)
                        .sum();
        return Math.max(size, openFileCost);
    }

    private long rangeWeight(List<DataFileMeta> files) {
        return Math.max(files.stream().mapToLong(this::fileWeight).sum(), openFileCost);
    }

    private long fileWeight(DataFileMeta file) {
        return BlobFileFormat.isBlobFile(file.fileName())
                ? countBlobSize ? file.fileSize() : openFileCost
                : file.fileSize();
    }

    private static boolean isSidecar(DataFileMeta file) {
        return BlobFileFormat.isBlobFile(file.fileName()) || isVectorStoreFile(file.fileName());
    }

    @Override
    public List<SplitGroup> splitForStreaming(List<DataFileMeta> files) {
        return splitForBatch(files);
    }
}
