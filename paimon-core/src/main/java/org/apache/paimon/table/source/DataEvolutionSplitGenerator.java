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

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;

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
        List<List<DataFileMeta>> files =
                split(
                        input,
                        DataFileMeta::fileName,
                        DataFileMeta::firstRowId,
                        DataFileMeta::rowCount,
                        DataFileMeta::maxSequenceNumber);
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

    public static <T> List<List<T>> split(
            List<T> files,
            Function<T, String> fileNameF,
            Function<T, Long> firstRowIdF,
            Function<T, Long> rowCountF,
            Function<T, Long> maxSequenceNumberF) {
        List<List<T>> splitByRowId = new ArrayList<>();
        // Sort files by firstRowId and then by maxSequenceNumber
        files.sort(
                Comparator.comparingLong(
                                (ToLongFunction<T>)
                                        value ->
                                                firstRowIdF.apply(value) == null
                                                        ? Long.MIN_VALUE
                                                        : firstRowIdF.apply(value))
                        .thenComparingInt(f -> isBlobFile(fileNameF.apply(f)) ? 1 : 0)
                        .thenComparing(
                                (f1, f2) -> {
                                    // If firstRowId is the same, we should read the file with
                                    // larger sequence number first. Because larger sequence number
                                    // file is more fresh
                                    return Long.compare(
                                            maxSequenceNumberF.apply(f2),
                                            maxSequenceNumberF.apply(f1));
                                }));

        files = filterBlob(files, fileNameF, firstRowIdF, rowCountF);

        // Split files by firstRowId
        long lastRowId = -1;
        long checkRowIdStart = 0;
        List<T> currentSplit = new ArrayList<>();
        for (int i = 0; i < files.size(); i++) {
            T file = files.get(i);
            Long firstRowId = firstRowIdF.apply(file);
            if (firstRowId == null) {
                splitByRowId.add(Collections.singletonList(file));
                continue;
            }
            if (!isBlobFile(fileNameF.apply(file)) && firstRowId != lastRowId) {
                if (!currentSplit.isEmpty()) {
                    splitByRowId.add(currentSplit);
                }
                if (firstRowId < checkRowIdStart) {
                    throw new IllegalStateException(
                            String.format(
                                    "There are overlapping files in the split: \n %s, the wrong file is: \n %s",
                                    files.subList(Math.max(0, i - 20), i).stream()
                                            .map(Object::toString)
                                            .collect(Collectors.joining(",")),
                                    file));
                }
                currentSplit = new ArrayList<>();
                lastRowId = firstRowId;
                checkRowIdStart = firstRowId + rowCountF.apply(file);
            }
            currentSplit.add(file);
        }
        if (!currentSplit.isEmpty()) {
            splitByRowId.add(currentSplit);
        }

        return splitByRowId;
    }

    private static <T> List<T> filterBlob(
            List<T> files,
            Function<T, String> fileNameF,
            Function<T, Long> firstRowIdF,
            Function<T, Long> rowCountF) {
        List<T> result = new ArrayList<>();
        long rowIdStart = -1;
        long rowIdEnd = -1;
        for (T file : files) {
            if (firstRowIdF.apply(file) == null) {
                result.add(file);
                continue;
            }
            if (!isBlobFile(fileNameF.apply(file))) {
                rowIdStart = firstRowIdF.apply(file);
                rowIdEnd = firstRowIdF.apply(file) + rowCountF.apply(file);
                result.add(file);
            } else {
                if (firstRowIdF.apply(file) >= rowIdStart && firstRowIdF.apply(file) < rowIdEnd) {
                    result.add(file);
                }
            }
        }
        return result;
    }
}
