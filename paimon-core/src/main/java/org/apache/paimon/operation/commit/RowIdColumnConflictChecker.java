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

package org.apache.paimon.operation.commit;

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RangeHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Detects row-id range conflicts only when written field ids overlap. The detection process is as
 * below:
 *
 * <ol>
 *   <li>Merge delta files by row range and calculate updated columns.
 *   <li>Sort those items by range.
 *   <li>For each checking files, do binary search to find overlapping ranges. If their updated
 *       columns also overlap, return conflicting result.
 * </ol>
 */
public class RowIdColumnConflictChecker {

    private final SchemaManager schemaManager;
    private final List<WriteRange> writeRanges;
    private final Map<Long, Map<String, Integer>> fieldIdByNameCache = new HashMap<>();

    private RowIdColumnConflictChecker(SchemaManager schemaManager, List<DataFileMeta> deltaFiles) {
        this.schemaManager = schemaManager;
        this.writeRanges = buildWriteRanges(deltaFiles);
    }

    public static RowIdColumnConflictChecker fromDataFiles(
            SchemaManager schemaManager, List<DataFileMeta> deltaFiles) {
        return new RowIdColumnConflictChecker(schemaManager, deltaFiles);
    }

    private List<WriteRange> buildWriteRanges(List<DataFileMeta> deltaFiles) {
        List<DataFileMeta> rowIdFiles =
                deltaFiles.stream()
                        .filter(file -> file.firstRowId() != null)
                        .collect(Collectors.toList());

        if (rowIdFiles.isEmpty()) {
            return Collections.emptyList();
        }

        // 1. merge overlapping ranges and calculate [Range, Set<FieldId>] tuples.
        RangeHelper<DataFileMeta> rangeHelper = new RangeHelper<>(DataFileMeta::nonNullRowIdRange);
        List<WriteRange> writeRanges = new ArrayList<>();
        for (List<DataFileMeta> group : rangeHelper.mergeOverlappingRanges(rowIdFiles)) {
            Range range = mergeRange(group);
            Set<Integer> fieldIds = new HashSet<>();
            for (DataFileMeta file : group) {
                addWriteFieldIds(fieldIds, file);
            }

            writeRanges.add(new WriteRange(range, fieldIds));
        }

        // 2. sort by range for binary search
        writeRanges.sort(
                Comparator.comparingLong((WriteRange writeRange) -> writeRange.range.from)
                        .thenComparingLong(writeRange -> writeRange.range.to));

        return writeRanges;
    }

    private void addWriteFieldIds(Set<Integer> fieldIds, DataFileMeta file) {
        List<String> writeCols = file.writeCols();
        if (writeCols == null) {
            fieldIds.addAll(
                    fieldIdByNameCache
                            .computeIfAbsent(file.schemaId(), this::fieldIdByName)
                            .values());
            return;
        }

        for (String writeCol : writeCols) {
            Integer fieldId = fieldId(file, writeCol);
            if (fieldId != null) {
                fieldIds.add(fieldId);
            }
        }
    }

    private static Range mergeRange(List<DataFileMeta> files) {
        long from = Long.MAX_VALUE;
        long to = Long.MIN_VALUE;
        for (DataFileMeta file : files) {
            Range range = file.nonNullRowIdRange();
            from = Math.min(from, range.from);
            to = Math.max(to, range.to);
        }
        return new Range(from, to);
    }

    boolean isEmpty() {
        return writeRanges.isEmpty();
    }

    /**
     * Check whether a committed incremental file entry conflicts with current committing delta
     * files. If an existing file has both overlapping row range and overlapping write fields, then
     * it conflicts.
     *
     * @param file committed incremental data file
     * @return true if conflict
     */
    boolean conflictsWith(DataFileMeta file) {
        Long firstRowId = file.firstRowId();
        if (firstRowId == null) {
            return false;
        }

        Range range = new Range(firstRowId, firstRowId + file.rowCount() - 1);
        int index = firstPossibleRange(range);
        while (index < writeRanges.size()) {
            WriteRange writeRange = writeRanges.get(index);
            if (writeRange.range.from > range.to) {
                return false;
            }
            // overlapping row range and overlapping write fields
            if (writeRange.range.hasIntersection(range)
                    && containsAnyWriteField(writeRange.fieldIds, file)) {
                return true;
            }
            index++;
        }
        return false;
    }

    /**
     * Binary search to find the first range whose `to` >= target range's `from`.
     *
     * @param range querying range
     * @return index of the first range
     */
    private int firstPossibleRange(Range range) {
        int low = 0;
        int high = writeRanges.size();
        while (low < high) {
            int mid = (low + high) >>> 1;
            if (writeRanges.get(mid).range.to < range.from) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }

    private boolean containsAnyWriteField(Set<Integer> fieldIds, DataFileMeta file) {
        List<String> writeCols = file.writeCols();
        // If write cols == null, it's a full-schema write
        if (writeCols == null) {
            return true;
        }

        for (String writeCol : writeCols) {
            Integer fieldId = fieldId(file, writeCol);
            if (fieldId != null && fieldIds.contains(fieldId)) {
                return true;
            }
        }
        return false;
    }

    private Integer fieldId(DataFileMeta file, String writeCol) {
        Integer fieldId =
                fieldIdByNameCache
                        .computeIfAbsent(file.schemaId(), this::fieldIdByName)
                        .get(writeCol);
        if (fieldId == null) {
            if (SpecialFields.isSystemField(writeCol)) {
                return null;
            }
            throw new RuntimeException(
                    String.format(
                            "Cannot find write column '%s' in schema %s.",
                            writeCol, file.schemaId()));
        }
        return fieldId;
    }

    private Map<String, Integer> fieldIdByName(long schemaId) {
        Map<String, Integer> fieldIdByName = new HashMap<>();
        for (DataField field : schemaManager.schema(schemaId).logicalRowType().getFields()) {
            fieldIdByName.put(field.name(), field.id());
        }
        return fieldIdByName;
    }

    /** Range and field id Set. */
    private static class WriteRange {

        private final Range range;
        private final Set<Integer> fieldIds;

        private WriteRange(Range range, Set<Integer> fieldIds) {
            this.range = range;
            this.fieldIds = fieldIds;
        }
    }
}
