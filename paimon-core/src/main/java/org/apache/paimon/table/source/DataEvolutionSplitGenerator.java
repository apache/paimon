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

import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
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
        Set<DataFileMeta> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        boolean hasSpanningSidecar =
                ranges.stream().flatMap(Collection::stream).anyMatch(file -> !seen.add(file));
        Function<List<DataFileMeta>, Long> weightFunc =
                files -> rangeWeight(files, hasSpanningSidecar);
        return BinPacking.packForOrdered(ranges, weightFunc, targetSplitSize).stream()
                .map(
                        f -> {
                            boolean rawConvertible =
                                    !hasSpanningSidecar
                                            && f.stream().allMatch(file -> file.size() == 1);
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

    private long rangeWeight(List<DataFileMeta> files, boolean hasSpanningSidecar) {
        if (hasSpanningSidecar) {
            List<DataFileMeta> normalFiles =
                    files.stream().filter(file -> !isSidecar(file)).collect(Collectors.toList());
            if (!normalFiles.isEmpty()) {
                return Math.max(
                        normalFiles.stream().mapToLong(DataFileMeta::fileSize).sum(), openFileCost);
            }
        }
        return Math.max(
                files.stream()
                        .mapToLong(
                                file ->
                                        BlobFileFormat.isBlobFile(file.fileName())
                                                ? countBlobSize ? file.fileSize() : openFileCost
                                                : file.fileSize())
                        .sum(),
                openFileCost);
    }

    private static boolean isSidecar(DataFileMeta file) {
        return BlobFileFormat.isBlobFile(file.fileName()) || isVectorStoreFile(file.fileName());
    }

    @Override
    public List<SplitGroup> splitForStreaming(List<DataFileMeta> files) {
        return splitForBatch(files);
    }
}
