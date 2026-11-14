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
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongFunction;
import java.util.stream.Collectors;

import static java.util.Comparator.comparingLong;
import static org.apache.paimon.types.BlobType.fieldNamesInBlobFile;
import static org.apache.paimon.types.VectorType.fieldNamesInVectorFile;
import static org.apache.paimon.utils.DataEvolutionUtils.fileFields;

/** Selects normal column-file versions to rewrite while retaining older column providers. */
final class DataEvolutionIncrementalCompactPlanner {

    private final CoreOptions options;
    private final RowType rowType;
    private final Set<Integer> currentFieldIds;
    private final LongFunction<TableSchema> schemaFetcher;

    DataEvolutionIncrementalCompactPlanner(FileStoreTable table) {
        this.options = table.coreOptions();
        this.rowType = table.rowType();
        Set<String> dedicatedFields = fieldNamesInBlobFile(rowType, options.blobInlineField());
        dedicatedFields.addAll(fieldNamesInVectorFile(rowType, options.withVectorFormat()));
        this.currentFieldIds =
                DataEvolutionIncrementalCompactTask.fieldIds(
                        rowType.getFields().stream()
                                .filter(field -> !table.partitionKeys().contains(field.name()))
                                .filter(field -> !dedicatedFields.contains(field.name()))
                                .collect(Collectors.toList()),
                        options.dataEvolutionNestedFieldEnabled());
        Map<Long, TableSchema> schemas = new HashMap<>();
        this.schemaFetcher = id -> schemas.computeIfAbsent(id, table.schemaManager()::schema);
    }

    /** Returns a newest-first prefix, the whole group for a full rewrite, or no files. */
    List<DataFileMeta> selectFiles(List<DataFileMeta> files, Range range, boolean splitLargeFile) {
        // Smaller output ranges cannot coexist with a retained normal version of this range.
        if (splitLargeFile
                || files.stream().anyMatch(file -> !file.nonNullRowIdRange().equals(range))) {
            return files;
        }
        long compactMinFileNum = options.compactionMinFileNum();
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
            if (suppliedFields.addAll(currentFields(file))) {
                // Count the entire file when any current column still needs it. Different
                // columns are useful data, not redundant versions of the oldest file.
                liveSize += file.fileSize();
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
        Set<Integer> writtenFields = new HashSet<>();
        for (DataFileMeta file : selected) {
            writtenFields.addAll(currentFields(file));
            if (options.dataEvolutionNestedFieldEnabled()) {
                for (DataField field : fileFields(schemaFetcher.apply(file.schemaId()), file)) {
                    if (field.type() instanceof RowType
                            && rowType.containsField(field.id())
                            && !currentFieldIds.containsAll(
                                    DataEvolutionIncrementalCompactTask.fieldIds(
                                            Collections.singletonList(field), true))) {
                        // Dropped leaves can still be hidden parent-nullness providers.
                        // Reading the whole group preserves those parents without replacing
                        // untouched siblings with newly manufactured NULLs.
                        return files;
                    }
                }
            }
        }
        if (writtenFields.isEmpty()) {
            return files;
        }
        if (options.dataEvolutionNestedFieldEnabled()) {
            RowType projected =
                    DataEvolutionIncrementalCompactTask.project(rowType, writtenFields, true);
            try {
                projected.collectLeafPaths(rowType);
            } catch (UnsupportedOperationException e) {
                // Schema evolution can make an old whole struct a deep partial struct, or
                // introduce an ambiguous dotted path. Such projections need a full rewrite.
                return files;
            }
        }
        return selected;
    }

    private Set<Integer> currentFields(DataFileMeta file) {
        Set<Integer> fields =
                DataEvolutionIncrementalCompactTask.fieldIds(
                        fileFields(schemaFetcher.apply(file.schemaId()), file),
                        options.dataEvolutionNestedFieldEnabled());
        fields.retainAll(currentFieldIds);
        return fields;
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
