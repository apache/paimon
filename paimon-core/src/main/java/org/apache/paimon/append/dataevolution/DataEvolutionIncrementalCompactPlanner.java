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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.io.DataFileMeta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Comparator.comparingLong;

/**
 * Selects a newest-first version prefix independently of the physical file format.
 *
 * <p>Callers provide current field IDs, whether a file has complete column values for its rows, and
 * whether the selected columns can be safely rewritten on their own.
 */
final class DataEvolutionIncrementalCompactPlanner {

    private final CoreOptions options;
    private final Function<DataFileMeta, Set<Integer>> currentFields;
    private final Predicate<DataFileMeta> completeColumnValues;
    private final Predicate<List<DataFileMeta>> canRewrite;

    DataEvolutionIncrementalCompactPlanner(
            CoreOptions options,
            Function<DataFileMeta, Set<Integer>> currentFields,
            Predicate<DataFileMeta> completeColumnValues,
            Predicate<List<DataFileMeta>> canRewrite) {
        this.options = options;
        this.currentFields = currentFields;
        this.completeColumnValues = completeColumnValues;
        this.canRewrite = canRewrite;
    }

    /** Returns a newest-first prefix, the whole group for a full rewrite, or no files. */
    List<DataFileMeta> selectFiles(List<DataFileMeta> files, long compactMinFileNum) {
        // A partial rewrite must supply the same rows as every retained version.
        if (!files.isEmpty()
                && files.stream()
                        .anyMatch(
                                file ->
                                        !file.nonNullRowIdRange()
                                                .equals(files.get(0).nonNullRowIdRange()))) {
            return files;
        }
        if (files.size() < compactMinFileNum) {
            return Collections.emptyList();
        }
        List<DataFileMeta> sorted = new ArrayList<>(files);
        sorted.sort(comparingLong(DataFileMeta::maxSequenceNumber).reversed());
        Set<Integer> suppliedFields = new HashSet<>();
        double totalSize = 0;
        double liveSize = 0;
        for (DataFileMeta file : sorted) {
            totalSize += file.fileSize();
            Set<Integer> fields = currentFields.apply(file);
            if (!suppliedFields.containsAll(fields)) {
                // Count the entire file when any current column still needs it. Different
                // columns are useful data, not redundant versions of the oldest file.
                liveSize += file.fileSize();
            }
            // A version containing placeholders cannot supersede older providers by field ID.
            if (completeColumnValues.test(file)) {
                suppliedFields.addAll(fields);
            }
        }
        if ((totalSize - liveSize) * 100 > options.maxSizeAmplificationPercent() * liveSize) {
            return files;
        }
        List<DataFileMeta> selected =
                pickIncrementalFiles(sorted, compactMinFileNum, options.sortedRunSizeRatio());
        if (selected.isEmpty() || selected.size() == files.size()) {
            return selected;
        }
        return canRewrite.test(selected) ? selected : files;
    }

    @VisibleForTesting
    static List<DataFileMeta> pickIncrementalFiles(
            List<DataFileMeta> files, long minFileNum, int sizeRatio) {
        if (files.size() < minFileNum || files.size() < 2) {
            return Collections.emptyList();
        }
        List<DataFileMeta> sorted = new ArrayList<>(files);
        sorted.sort(comparingLong(DataFileMeta::maxSequenceNumber).reversed());
        long minCandidates = Math.max(1L, sorted.size() - minFileNum + 1L);
        int count = 0;
        double size = 0;
        while (count < sorted.size()) {
            int end = count + 1;
            long sequence = sorted.get(count).maxSequenceNumber();
            double runSize = sorted.get(count).fileSize();
            // Never split files committed at the same version at the selection boundary.
            while (end < sorted.size() && sorted.get(end).maxSequenceNumber() == sequence) {
                runSize += sorted.get(end++).fileSize();
            }
            if (count >= minCandidates && size * (100.0 + sizeRatio) / 100.0 < runSize) {
                break;
            }
            size += runSize;
            count = end;
        }
        return count < 2 ? Collections.emptyList() : new ArrayList<>(sorted.subList(0, count));
    }
}
