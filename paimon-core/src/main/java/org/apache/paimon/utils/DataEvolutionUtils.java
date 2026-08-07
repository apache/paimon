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
import org.apache.paimon.table.source.AllColumns;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.KnownWrittenColumns;
import org.apache.paimon.table.source.WrittenColumns;
import org.apache.paimon.types.DataField;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

    /** Collect written field ids from data files in the selected splits. */
    public static WrittenColumns collectWrittenColumns(
            Collection<DataSplit> splits, Function<Long, TableSchema> schemaLoader) {
        Set<Integer> fieldIds = new TreeSet<>();
        Map<Long, Map<String, Integer>> fieldIdByNameCache = new HashMap<>();
        Map<Pair<Long, List<String>>, Set<Integer>> fieldIdsCache = new HashMap<>();
        for (DataSplit split : splits) {
            for (DataFileMeta file : split.dataFiles()) {
                try {
                    Pair<Long, List<String>> cacheKey = Pair.of(file.schemaId(), file.writeCols());
                    Set<Integer> fileFieldIds = fieldIdsCache.get(cacheKey);
                    if (fileFieldIds == null) {
                        fileFieldIds = computeFileFieldIds(schemaLoader, fieldIdByNameCache, file);
                        fieldIdsCache.put(cacheKey, fileFieldIds);
                        fieldIds.addAll(fileFieldIds);
                    }
                } catch (RuntimeException e) {
                    return AllColumns.INSTANCE;
                }
            }
        }
        return new KnownWrittenColumns(fieldIds);
    }

    private static Set<Integer> computeFileFieldIds(
            Function<Long, TableSchema> schemaLoader,
            Map<Long, Map<String, Integer>> fieldIdByNameCache,
            DataFileMeta file) {
        Map<String, Integer> fieldIdByName =
                fieldIdByNameCache.computeIfAbsent(
                        file.schemaId(),
                        schemaId -> {
                            TableSchema fileSchema = schemaLoader.apply(schemaId);
                            if (fileSchema == null) {
                                throw new IllegalArgumentException(
                                        "Cannot find schema " + schemaId);
                            }

                            Map<String, Integer> fieldIds = new HashMap<>();
                            for (DataField field : fileSchema.fields()) {
                                fieldIds.put(field.name(), field.id());
                            }
                            return fieldIds;
                        });

        List<String> writeCols = file.writeCols();
        if (writeCols == null) {
            return new TreeSet<>(fieldIdByName.values());
        }

        Set<Integer> fieldIds = new TreeSet<>();
        for (String writeCol : writeCols) {
            Integer fieldId = fieldIdByName.get(writeCol);
            if (fieldId == null) {
                checkArgument(
                        SpecialFields.isSystemField(writeCol),
                        "Cannot find write column '%s' in schema %s.",
                        writeCol,
                        file.schemaId());
            } else {
                fieldIds.add(fieldId);
            }
        }
        return fieldIds;
    }

    /**
     * Table field ids physically present in a file, resolved through the schema used to write it.
     */
    public static Set<Integer> fileFieldIds(
            Function<Long, TableSchema> scanTableSchema, DataFileMeta file) {
        TableSchema schema = scanTableSchema.apply(file.schemaId());
        List<String> writeCols = file.writeCols();
        Set<String> writeColNames = writeCols == null ? null : new HashSet<>(writeCols);
        Set<Integer> ids = new HashSet<>();
        for (DataField field : schema.fields()) {
            // writeCols may also contain physical row-tracking fields outside the table schema.
            if (writeColNames == null || writeColNames.contains(field.name())) {
                ids.add(field.id());
            }
        }
        return ids;
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
}
