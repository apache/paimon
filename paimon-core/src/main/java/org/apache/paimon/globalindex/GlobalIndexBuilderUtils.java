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

package org.apache.paimon.globalindex;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.Range;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Utils for global index build. */
public class GlobalIndexBuilderUtils {

    private static final Logger LOG = LoggerFactory.getLogger(GlobalIndexBuilderUtils.class);

    public static List<IndexFileMeta> toIndexFileMetas(
            FileIO fileIO,
            IndexPathFactory indexPathFactory,
            CoreOptions options,
            Range range,
            int indexFieldId,
            String indexType,
            List<ResultEntry> entries)
            throws IOException {
        return toIndexFileMetas(
                fileIO, indexPathFactory, options, range, indexFieldId, null, indexType, entries);
    }

    /**
     * Builds the index file metas. The first column in {@code fields} is treated as the primary
     * index column (e.g. the first column in {@code CREATE ... INDEX ON (a, b, c)}) and is stored
     * as {@code indexFieldId}; the remaining columns go into {@code extraFieldIds}. Callers must
     * pass {@code fields} in the intended column order.
     */
    public static List<IndexFileMeta> toIndexFileMetas(
            FileIO fileIO,
            IndexPathFactory indexPathFactory,
            CoreOptions options,
            Range range,
            List<DataField> fields,
            String indexType,
            List<ResultEntry> entries)
            throws IOException {
        // The first column is the primary index column and is stored as indexFieldId; the
        // remaining columns (if any) go into extraFieldIds.
        int indexFieldId = fields.get(0).id();
        int[] extraFieldIds =
                fields.size() > 1
                        ? fields.subList(1, fields.size()).stream()
                                .mapToInt(DataField::id)
                                .toArray()
                        : null;
        return toIndexFileMetas(
                fileIO,
                indexPathFactory,
                options,
                range,
                indexFieldId,
                extraFieldIds,
                indexType,
                entries);
    }

    private static List<IndexFileMeta> toIndexFileMetas(
            FileIO fileIO,
            IndexPathFactory indexPathFactory,
            CoreOptions options,
            Range range,
            int indexFieldId,
            @Nullable int[] extraFieldIds,
            String indexType,
            List<ResultEntry> entries)
            throws IOException {
        List<IndexFileMeta> results = new ArrayList<>();
        for (ResultEntry entry : entries) {
            String fileName = entry.fileName();
            long fileSize = fileIO.getFileSize(indexPathFactory.toPath(fileName));
            GlobalIndexMeta globalIndexMeta =
                    new GlobalIndexMeta(
                            range.from, range.to, indexFieldId, extraFieldIds, entry.meta());

            Path externalPathDir = options.globalIndexExternalPath();
            String externalPathString = null;
            if (externalPathDir != null) {
                Path externalPath = new Path(externalPathDir, fileName);
                externalPathString = externalPath.toString();
            }
            IndexFileMeta indexFileMeta =
                    new IndexFileMeta(
                            indexType,
                            fileName,
                            fileSize,
                            entry.rowCount(),
                            globalIndexMeta,
                            externalPathString);
            results.add(indexFileMeta);
        }
        return results;
    }

    public static GlobalIndexWriter createIndexWriter(
            FileStoreTable table, String indexType, DataField indexField, Options options)
            throws IOException {
        GlobalIndexer globalIndexer = GlobalIndexer.create(indexType, indexField, options);
        return globalIndexer.createWriter(createGlobalIndexFileReadWrite(table));
    }

    public static GlobalIndexWriter createIndexWriter(
            FileStoreTable table,
            String indexType,
            DataField dataField,
            List<DataField> extraFields,
            Options options)
            throws IOException {
        GlobalIndexer globalIndexer =
                GlobalIndexer.create(indexType, dataField, extraFields, options);
        return globalIndexer.createWriter(createGlobalIndexFileReadWrite(table));
    }

    /**
     * Find the minimum firstRowId among files whose schema does not contain all index columns.
     * Files at or beyond this rowId cannot be indexed because the column was added later via ALTER
     * TABLE.
     *
     * @return the boundary rowId, or {@link Long#MAX_VALUE} if all files contain the columns
     */
    public static long findMinNonIndexableRowId(
            SchemaManager schemaManager, List<ManifestEntry> entries, List<String> indexColumns) {
        Map<Long, Boolean> schemaContainsColumns = new HashMap<>();
        long minRowId = Long.MAX_VALUE;
        long minSchemaId = -1;
        for (ManifestEntry entry : entries) {
            long sid = entry.file().schemaId();
            boolean contains =
                    schemaContainsColumns.computeIfAbsent(
                            sid,
                            id -> schemaManager.schema(id).fieldNames().containsAll(indexColumns));
            if (!contains && entry.file().firstRowId() != null) {
                long rowId = entry.file().nonNullFirstRowId();
                if (rowId < minRowId) {
                    minRowId = rowId;
                    minSchemaId = sid;
                }
            }
        }
        if (minRowId != Long.MAX_VALUE) {
            List<String> schemaFields = schemaManager.schema(minSchemaId).fieldNames();
            List<String> missingColumns = new ArrayList<>();
            for (String col : indexColumns) {
                if (!schemaFields.contains(col)) {
                    missingColumns.add(col);
                }
            }
            LOG.info(
                    "Found non-indexable files: schemaId={} missing columns {}, boundaryRowId={}.",
                    minSchemaId,
                    missingColumns,
                    minRowId);
        }
        return minRowId;
    }

    /** Keep only entries whose firstRowId is strictly less than the given boundary. */
    public static List<ManifestEntry> filterEntriesBefore(
            List<ManifestEntry> entries, long boundaryRowId) {
        if (boundaryRowId == Long.MAX_VALUE) {
            return entries;
        }
        List<ManifestEntry> result = new ArrayList<>();
        for (ManifestEntry entry : entries) {
            if (entry.file().firstRowId() != null
                    && entry.file().nonNullFirstRowId() < boundaryRowId) {
                result.add(entry);
            }
        }
        LOG.info(
                "Filtered {} files to {} indexable files (boundaryRowId={}).",
                entries.size(),
                result.size(),
                boundaryRowId);
        return result;
    }

    private static GlobalIndexFileReadWrite createGlobalIndexFileReadWrite(FileStoreTable table) {
        IndexPathFactory indexPathFactory = table.store().pathFactory().globalIndexFileFactory();
        return new GlobalIndexFileReadWrite(table.fileIO(), indexPathFactory);
    }
}
