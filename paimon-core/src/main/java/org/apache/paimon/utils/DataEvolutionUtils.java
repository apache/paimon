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

package org.apache.paimon.utils;

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataField;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Util class for data evolution. */
public class DataEvolutionUtils {

    /**
     * Collect exact written field ids; an empty list is exact and an empty optional is unresolved.
     */
    public static Optional<List<Integer>> collectWrittenColumnIds(
            Collection<DataSplit> splits, Function<Long, TableSchema> schemaLoader) {
        Set<Integer> fieldIds = new TreeSet<>();
        Map<Long, List<DataField>> schemaFieldsCache = new HashMap<>();
        Map<Pair<Long, List<String>>, Set<Integer>> fieldIdsCache = new HashMap<>();
        try {
            for (DataSplit split : splits) {
                for (DataFileMeta file : split.dataFiles()) {
                    Pair<Long, List<String>> cacheKey = Pair.of(file.schemaId(), file.writeCols());
                    Set<Integer> fileFieldIds = fieldIdsCache.get(cacheKey);
                    if (fileFieldIds == null) {
                        List<DataField> schemaFields =
                                schemaFieldsCache.computeIfAbsent(
                                        file.schemaId(),
                                        schemaId -> {
                                            TableSchema schema = schemaLoader.apply(schemaId);
                                            checkArgument(
                                                    schema != null,
                                                    "Cannot find schema %s.",
                                                    schemaId);
                                            return schema.fields();
                                        });
                        fileFieldIds = resolveFileFieldIds(schemaFields, file, true);
                        fieldIdsCache.put(cacheKey, fileFieldIds);
                    }
                    fieldIds.addAll(fileFieldIds);
                }
            }
        } catch (RuntimeException e) {
            return Optional.empty();
        }
        return Optional.of(Collections.unmodifiableList(new ArrayList<>(fieldIds)));
    }

    /**
     * Table field ids physically present in a file, resolved through the schema used to write it.
     */
    public static Set<Integer> fileFieldIds(
            Function<Long, TableSchema> scanTableSchema, DataFileMeta file) {
        return resolveFileFieldIds(scanTableSchema.apply(file.schemaId()).fields(), file, false);
    }

    private static Set<Integer> resolveFileFieldIds(
            List<DataField> schemaFields, DataFileMeta file, boolean strict) {
        List<String> writeCols = file.writeCols();
        Set<String> writeColNames = writeCols == null ? null : new HashSet<>(writeCols);
        Set<String> unresolved =
                strict && writeColNames != null ? new HashSet<>(writeColNames) : null;
        Set<Integer> ids = new HashSet<>();
        for (DataField field : schemaFields) {
            // writeCols may also contain physical row-tracking fields outside the table schema.
            if (writeColNames == null || writeColNames.contains(field.name())) {
                ids.add(field.id());
                if (unresolved != null) {
                    unresolved.remove(field.name());
                }
            }
        }

        if (unresolved != null) {
            unresolved.removeIf(SpecialFields::isSystemField);
            checkArgument(
                    unresolved.isEmpty(),
                    "Cannot find write columns %s in schema %s.",
                    unresolved,
                    file.schemaId());
        }
        return ids;
    }

    /** Table fields physically present in a file, in their physical write order. */
    public static List<DataField> fileFields(
            Function<Long, TableSchema> scanTableSchema, DataFileMeta file) {
        TableSchema schema = scanTableSchema.apply(file.schemaId());
        List<String> writeCols = file.writeCols();
        if (writeCols == null) {
            return schema.fields();
        }

        Map<String, DataField> fieldsByName = new HashMap<>();
        for (DataField field : schema.fields()) {
            fieldsByName.put(field.name(), field);
        }
        List<DataField> fields = new ArrayList<>();
        for (String writeCol : writeCols) {
            // writeCols may also contain physical row-tracking fields outside the table schema.
            DataField field = fieldsByName.get(writeCol);
            if (field != null) {
                fields.add(field);
            }
        }
        return fields;
    }

    /** Returns the latest sequence known for a physical field position in the file. */
    public static long fieldMaxSequenceNumber(
            DataFileMeta file,
            @Nullable long[] columnSequences,
            int fieldPosition,
            int physicalFieldCount) {
        if (columnSequences == null || columnSequences.length != physicalFieldCount) {
            return file.maxSequenceNumber();
        }
        return columnSequences[fieldPosition];
    }

    /**
     * Retrieve the anchor file of a row range group. Always the oldest normal file. Files are
     * compared by (max_seq, fileName) pairs.
     */
    public static <T> T retrieveAnchorFile(
            Collection<T> entries, Function<T, DataFileMeta> fileMetaFunc) {
        T anchor = null;
        DataFileMeta minMeta = null;

        Comparator<DataFileMeta> fileComparator =
                Comparator.comparingLong(DataFileMeta::maxSequenceNumber)
                        .thenComparing(DataFileMeta::fileName);

        for (T entry : entries) {
            DataFileMeta meta = fileMetaFunc.apply(entry);
            if (isBlobFile(meta.fileName()) || isVectorStoreFile(meta.fileName())) {
                continue;
            }

            if (minMeta == null || fileComparator.compare(meta, minMeta) < 0) {
                minMeta = meta;
                anchor = entry;
            }
        }

        checkState(
                anchor != null,
                "Data-evolution deletion vectors should have a normal anchor file in each row range group.");
        return anchor;
    }

    /** Check files row ranges. */
    public static Range checkContiguousRowRange(List<DataFileMeta> files) {
        checkArgument(!files.isEmpty(), "%s should not be empty.", "Data evolution compact files");
        List<Range> ranges =
                files.stream().map(DataFileMeta::nonNullRowIdRange).collect(Collectors.toList());
        List<Range> merged = Range.sortAndMergeOverlap(ranges, true);
        checkArgument(
                merged.size() == 1,
                "%s should have a contiguous row range, but got %s.",
                "Data evolution compact files",
                merged);
        return merged.get(0);
    }

    /**
     * Groups data-evolution files by normal-file row ranges.
     *
     * <p>Blob and vector files may span several adjacent normal files. Such sidecar files are
     * included in every anchor group they intersect, so each group can select just its normal-file
     * range without duplicating the physical sidecar.
     */
    public static <T> List<List<T>> groupByNormalFileRange(
            List<T> entries, Function<T, DataFileMeta> fileMetaFunc) {
        Map<T, Integer> originalOrder = new IdentityHashMap<>();
        List<T> normal = new ArrayList<>();
        List<T> sidecars = new ArrayList<>();
        for (int i = 0; i < entries.size(); i++) {
            T entry = entries.get(i);
            originalOrder.put(entry, i);
            DataFileMeta file = fileMetaFunc.apply(entry);
            if (isBlobFile(file.fileName()) || isVectorStoreFile(file.fileName())) {
                sidecars.add(entry);
            } else {
                normal.add(entry);
            }
        }

        if (normal.isEmpty()) {
            return new RangeHelper<T>(entry -> fileMetaFunc.apply(entry).nonNullRowIdRange())
                    .mergeOverlappingRanges(entries);
        }

        normal.sort(
                Comparator.<T>comparingLong(entry -> fileMetaFunc.apply(entry).nonNullFirstRowId())
                        .thenComparingLong(
                                entry -> fileMetaFunc.apply(entry).nonNullRowIdRange().to));
        List<List<T>> groups = new ArrayList<>();
        List<Range> groupRanges = new ArrayList<>();
        for (T entry : normal) {
            Range range = fileMetaFunc.apply(entry).nonNullRowIdRange();
            if (groups.isEmpty() || !range.equals(groupRanges.get(groupRanges.size() - 1))) {
                if (!groupRanges.isEmpty()) {
                    Range previous = groupRanges.get(groupRanges.size() - 1);
                    checkArgument(
                            !previous.hasIntersection(range),
                            "Normal data files have overlapping but different row ranges: %s and %s.",
                            previous,
                            range);
                }
                groups.add(new ArrayList<>());
                groupRanges.add(range);
            }
            groups.get(groups.size() - 1).add(entry);
        }

        List<T> unanchored = new ArrayList<>();
        for (T sidecar : sidecars) {
            Range sidecarRange = fileMetaFunc.apply(sidecar).nonNullRowIdRange();
            int first = firstRangeEndingAtOrAfter(groupRanges, sidecarRange.from);
            boolean attached = false;
            for (int i = first;
                    i < groupRanges.size() && groupRanges.get(i).from <= sidecarRange.to;
                    i++) {
                if (groupRanges.get(i).hasIntersection(sidecarRange)) {
                    groups.get(i).add(sidecar);
                    attached = true;
                }
            }
            if (!attached) {
                unanchored.add(sidecar);
            }
        }

        if (!unanchored.isEmpty()) {
            groups.addAll(
                    new RangeHelper<T>(entry -> fileMetaFunc.apply(entry).nonNullRowIdRange())
                            .mergeOverlappingRanges(unanchored));
            groups.sort(
                    Comparator.comparingLong(
                            group ->
                                    group.stream()
                                            .map(fileMetaFunc)
                                            .map(DataFileMeta::nonNullRowIdRange)
                                            .mapToLong(range -> range.from)
                                            .min()
                                            .orElse(Long.MAX_VALUE)));
        }
        for (List<T> group : groups) {
            group.sort(Comparator.comparingInt(originalOrder::get));
        }
        return groups;
    }

    private static int firstRangeEndingAtOrAfter(List<Range> ranges, long rowId) {
        int low = 0;
        int high = ranges.size();
        while (low < high) {
            int middle = (low + high) >>> 1;
            if (ranges.get(middle).to < rowId) {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        return low;
    }
}
