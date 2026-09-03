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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
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
            Collection<DataSplit> splits,
            Function<Long, List<DataField>> schemaFieldsLoader,
            Function<Long, Boolean> nestedFieldEnabledLoader) {
        return collectWrittenColumnIds(splits, schemaFieldsLoader, null, nestedFieldEnabledLoader);
    }

    /** Collect exact written field ids using the physical null-write-cols schema. */
    public static Optional<List<Integer>> collectWrittenColumnIds(
            Collection<DataSplit> splits, Function<Long, TableSchema> schemaLoader) {
        Map<Long, TableSchema> schemaCache = new HashMap<>();
        Function<Long, TableSchema> cachedSchemaLoader =
                schemaId -> schemaCache.computeIfAbsent(schemaId, schemaLoader);
        return collectWrittenColumnIds(
                splits,
                schemaId -> cachedSchemaLoader.apply(schemaId).fields(),
                schemaId -> cachedSchemaLoader.apply(schemaId).dataFileSchema(null).fields(),
                schemaId ->
                        new CoreOptions(cachedSchemaLoader.apply(schemaId).options())
                                .dataEvolutionNestedFieldEnabled());
    }

    private static Optional<List<Integer>> collectWrittenColumnIds(
            Collection<DataSplit> splits,
            Function<Long, List<DataField>> schemaFieldsLoader,
            @Nullable Function<Long, List<DataField>> nullWriteSchemaFieldsLoader,
            Function<Long, Boolean> nestedFieldEnabledLoader) {
        Set<Integer> fieldIds = new TreeSet<>();
        Map<Long, List<DataField>> schemaFieldsCache = new HashMap<>();
        Map<Long, List<DataField>> nullWriteSchemaFieldsCache = new HashMap<>();
        Map<Long, Boolean> nestedFieldEnabledCache = new HashMap<>();
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
                                            List<DataField> loaded =
                                                    schemaFieldsLoader.apply(schemaId);
                                            checkArgument(
                                                    loaded != null,
                                                    "Cannot find schema %s.",
                                                    schemaId);
                                            return loaded;
                                        });
                        List<DataField> nullWriteSchemaFields = schemaFields;
                        if (file.writeCols() == null && nullWriteSchemaFieldsLoader != null) {
                            nullWriteSchemaFields =
                                    nullWriteSchemaFieldsCache.computeIfAbsent(
                                            file.schemaId(),
                                            schemaId -> {
                                                List<DataField> loaded =
                                                        nullWriteSchemaFieldsLoader.apply(schemaId);
                                                checkArgument(
                                                        loaded != null,
                                                        "Cannot find schema %s.",
                                                        schemaId);
                                                return loaded;
                                            });
                        }
                        boolean nestedFieldEnabled =
                                nestedFieldEnabledCache.computeIfAbsent(
                                        file.schemaId(), nestedFieldEnabledLoader);
                        fileFieldIds =
                                resolveFileFieldIds(
                                        schemaFields,
                                        nullWriteSchemaFields,
                                        file,
                                        nestedFieldEnabled,
                                        true);
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
            List<DataField> schemaFields, DataFileMeta file, boolean nestedFieldEnabled) {
        return resolveFileFieldIds(schemaFields, schemaFields, file, nestedFieldEnabled, false);
    }

    public static Set<Integer> fileFieldIds(TableSchema schema, DataFileMeta file) {
        boolean nestedFieldEnabled =
                new CoreOptions(schema.options()).dataEvolutionNestedFieldEnabled();
        return resolveFileFieldIds(
                schema.fields(),
                schema.dataFileSchema(null).fields(),
                file,
                nestedFieldEnabled,
                false);
    }

    private static Set<Integer> resolveFileFieldIds(
            List<DataField> schemaFields,
            List<DataField> nullWriteSchemaFields,
            DataFileMeta file,
            boolean nestedFieldEnabled,
            boolean strict) {
        List<String> writeCols = file.writeCols();
        Set<Integer> ids = new HashSet<>();
        if (writeCols == null) {
            for (DataField field : nullWriteSchemaFields) {
                ids.add(field.id());
            }
            return ids;
        }

        Map<String, DataField> byName = new HashMap<>();
        for (DataField field : schemaFields) {
            byName.put(field.name(), field);
        }
        Set<String> unresolved = strict ? new HashSet<>() : null;
        for (String writeCol : writeCols) {
            // A write column is either a plain top-level name or, for sub-field-level data
            // evolution, a dotted path into a nested column such as "nest.a"; both identify the
            // same top-level field. Try the exact name first so a column whose own name contains a
            // dot is not split, matching RowType#projectByPaths.
            DataField field = byName.get(writeCol);
            if (field == null && nestedFieldEnabled) {
                int dot = writeCol.indexOf('.');
                if (dot > 0) {
                    field = byName.get(writeCol.substring(0, dot));
                }
            }
            // writeCols may also contain physical row-tracking fields outside the table schema.
            if (field != null) {
                ids.add(field.id());
            } else if (unresolved != null) {
                unresolved.add(writeCol);
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
            List<DataField> schemaFields, DataFileMeta file, boolean nestedFieldEnabled) {
        return fileFields(schemaFields, schemaFields, file, nestedFieldEnabled);
    }

    /** Table fields physically present in a file, including compact null-write-cols metadata. */
    public static List<DataField> fileFields(TableSchema schema, DataFileMeta file) {
        return fileFields(
                schema.fields(),
                schema.dataFileSchema(null).fields(),
                file,
                new CoreOptions(schema.options()).dataEvolutionNestedFieldEnabled());
    }

    /**
     * Returns the columns of a partial data file, materializing compact null write-column metadata
     * when necessary.
     *
     * <p>An empty optional means that a null value still denotes a true full-schema file. This
     * distinction is required by callers which only operate on partial updates.
     */
    public static Optional<List<String>> partialFileWriteCols(
            TableSchema schema, DataFileMeta file) {
        if (file.writeCols() != null) {
            return Optional.of(file.writeCols());
        }

        List<DataField> physicalFields = schema.dataFileSchema(null).fields();
        if (physicalFields.size() == schema.fields().size()) {
            return Optional.empty();
        }
        return Optional.of(
                physicalFields.stream().map(DataField::name).collect(Collectors.toList()));
    }

    private static List<DataField> fileFields(
            List<DataField> schemaFields,
            List<DataField> nullWriteSchemaFields,
            DataFileMeta file,
            boolean nestedFieldEnabled) {
        List<String> writeCols = file.writeCols();
        if (writeCols == null) {
            return nullWriteSchemaFields;
        }

        if (nestedFieldEnabled) {
            Set<String> fieldNames =
                    schemaFields.stream().map(DataField::name).collect(Collectors.toSet());
            List<String> tableWriteCols =
                    writeCols.stream()
                            .filter(
                                    writeCol -> {
                                        if (fieldNames.contains(writeCol)) {
                                            return true;
                                        }
                                        int dot = writeCol.indexOf('.');
                                        return dot > 0
                                                && fieldNames.contains(writeCol.substring(0, dot));
                                    })
                            .collect(Collectors.toList());
            return new RowType(schemaFields).projectByPaths(tableWriteCols).getFields();
        }

        Map<String, DataField> fieldsByName = new HashMap<>();
        for (DataField field : schemaFields) {
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
}
