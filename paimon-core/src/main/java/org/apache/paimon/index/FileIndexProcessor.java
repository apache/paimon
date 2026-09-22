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

package org.apache.paimon.index;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexCommon;
import org.apache.paimon.fileindex.FileIndexFormat;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileIndexWriter;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.SpillableIndexOutputStream;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DataFilePathFactories;
import org.apache.paimon.utils.FileStorePathFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.io.DataFilePathFactory.createNewFileIndexFilePath;
import static org.apache.paimon.io.DataFilePathFactory.dataFileToFileIndexPath;

/** Does the file index rewrite. */
public class FileIndexProcessor {

    private final FileStoreTable table;
    private final FileIndexOptions fileIndexOptions;
    private final FileIO fileIO;
    private final FileStorePathFactory pathFactory;
    private final DataFilePathFactories pathFactories;
    private final SchemaCache schemaInfoCache;
    private final int sizeInMeta;

    public FileIndexProcessor(FileStoreTable table) {
        this.table = table;
        this.fileIndexOptions = table.coreOptions().indexColumnsOptions();
        this.fileIO = table.fileIO();
        this.pathFactory = table.store().pathFactory();
        this.pathFactories = new DataFilePathFactories(pathFactory);
        this.schemaInfoCache = new SchemaCache(fileIndexOptions, table.schemaManager());
        this.sizeInMeta = table.coreOptions().fileIndexInManifestThreshold();
    }

    public DataFileMeta process(BinaryRow partition, int bucket, ManifestEntry manifestEntry)
            throws IOException {
        DataFileMeta dataFileMeta = manifestEntry.file();
        DataFilePathFactory dataFilePathFactory = pathFactories.get(partition, bucket);
        SchemaInfo schemaInfo = schemaInfoCache.schemaInfo(dataFileMeta.schemaId());
        List<String> extras = new ArrayList<>(dataFileMeta.extraFiles());
        List<String> indexFiles =
                dataFileMeta.extraFiles().stream()
                        .filter(name -> name.endsWith(DataFilePathFactory.INDEX_PATH_SUFFIX))
                        .collect(Collectors.toList());
        extras.removeAll(indexFiles);
        Path newIndexPath;
        FileIndexFormat.Reader sourceReader = null;
        if (!indexFiles.isEmpty()) {
            Path sourcePath = dataFilePathFactory.toAlignedPath(indexFiles.get(0), dataFileMeta);
            long sourceLength = fileIO.getFileStatus(sourcePath).getLen();
            sourceReader =
                    FileIndexFormat.createReader(
                            fileIO.newInputStream(sourcePath), schemaInfo.fileSchema, sourceLength);
            newIndexPath = createNewFileIndexFilePath(sourcePath);
        } else {
            newIndexPath = dataFileToFileIndexPath(dataFilePathFactory.toPath(dataFileMeta));
        }

        try (FileIndexFormat.Reader oldReader = sourceReader) {
            Map<String, Map<String, FileIndexFormat.Payload>> entries = new HashMap<>();
            if (oldReader != null) {
                for (FileIndexFormat.FileIndexMeta meta : oldReader.indexMetas()) {
                    String column = meta.columnName();
                    String type = meta.indexType();
                    if (schemaInfo.projectedColFullNames.contains(column)
                            && schemaInfo
                                    .projectedIndexTypes
                                    .getOrDefault(column, Collections.emptySet())
                                    .contains(type)) {
                        entries.computeIfAbsent(column, ignored -> new HashMap<>())
                                .put(
                                        type,
                                        meta.empty()
                                                ? null
                                                : output ->
                                                        oldReader.copyPayload(
                                                                column, type, output));
                    }
                }
            }

            // Collect the new writers, not their serialized payloads.
            @SuppressWarnings("resource")
            DataFileIndexWriter newIndexes =
                    DataFileIndexWriter.create(
                            fileIO,
                            newIndexPath,
                            schemaInfo.fileSchema.project(schemaInfo.projectedIndexCols),
                            fileIndexOptions,
                            schemaInfo.colNameMapping);
            if (newIndexes != null) {
                // projectedIndexCols index into the file schema. withProjection would re-interpret
                // them against the current table schema, so a schema change that shifts columns
                // would rebuild the index over the wrong column.
                RowType indexReadType = schemaInfo.fileSchema.project(schemaInfo.projectedIndexCols);
                try (RecordReader<InternalRow> reader =
                        table.newReadBuilder()
                                .withReadType(indexReadType)
                                .newRead()
                                .createReader(
                                        DataSplit.builder()
                                                .withPartition(partition)
                                                .withBucket(bucket)
                                                .withBucketPath(
                                                        pathFactory
                                                                .bucketPath(partition, bucket)
                                                                .toString())
                                                .withTotalBuckets(manifestEntry.totalBuckets())
                                                .withDataFiles(
                                                        Collections.singletonList(dataFileMeta))
                                                .rawConvertible(true)
                                                .build())) {
                    reader.forEachRemaining(newIndexes::write);
                }
                newIndexes.forEachIndex(
                        (column, type, writer) ->
                                entries.computeIfAbsent(column, ignored -> new HashMap<>())
                                        .put(type, writer == null ? null : writer::writeTo));
            }

            if (entries.isEmpty()) {
                return dataFileMeta.copy(extras).copy((byte[]) null);
            }

            SpillableIndexOutputStream output =
                    new SpillableIndexOutputStream(fileIO, newIndexPath, sizeInMeta);
            try {
                try (FileIndexFormat.Writer writer =
                        FileIndexFormat.createWriter(output, fileIndexOptions.formatVersion())) {
                    for (Map.Entry<String, Map<String, FileIndexFormat.Payload>> column :
                            entries.entrySet()) {
                        for (Map.Entry<String, FileIndexFormat.Payload> index :
                                column.getValue().entrySet()) {
                            writer.writeIndex(column.getKey(), index.getKey(), index.getValue());
                        }
                    }
                    writer.finish();
                }
                if (output.spilled()) {
                    extras.add(newIndexPath.getName());
                    return dataFileMeta.copy(extras).copy((byte[]) null);
                }
                return dataFileMeta.copy(extras).copy(output.embeddedBytes());
            } catch (IOException | RuntimeException e) {
                try {
                    output.abort();
                } catch (IOException cleanupFailure) {
                    e.addSuppressed(cleanupFailure);
                }
                throw e;
            }
        }
    }

    /** Schema id to specified information related to schema. */
    private static class SchemaCache {

        private final FileIndexOptions fileIndexOptions;
        private final SchemaManager schemaManager;
        private final TableSchema currentSchema;
        private final Map<Long, SchemaInfo> schemaInfos;
        private final Set<Long> fileSchemaIds;

        public SchemaCache(FileIndexOptions fileIndexOptions, SchemaManager schemaManager) {
            this.fileIndexOptions = fileIndexOptions;
            this.schemaManager = schemaManager;
            this.currentSchema = schemaManager.latest().orElseThrow(RuntimeException::new);
            this.schemaInfos = new HashMap<>();
            this.fileSchemaIds = new HashSet<>();
        }

        public SchemaInfo schemaInfo(long schemaId) {
            if (!fileSchemaIds.contains(schemaId)) {
                RowType fileSchema = schemaManager.schema(schemaId).logicalRowType();

                @Nullable
                Map<String, String> colNameMapping =
                        schemaId == currentSchema.id()
                                ? null
                                : createIndexNameMapping(
                                        currentSchema.fields(), fileSchema.getFields());

                // several nested columns can share one top level map column, and the projection
                // must not repeat it: RowType rejects duplicate field names
                Set<String> projectedColNames = new LinkedHashSet<>();
                Set<String> projectedColFullNames = new HashSet<>();
                Map<String, Set<String>> projectedIndexTypes = new HashMap<>();
                for (Map.Entry<FileIndexOptions.Column, Map<String, Options>> entry :
                        fileIndexOptions.entrySet()) {
                    FileIndexOptions.Column column = entry.getKey();
                    String columnName;
                    if (colNameMapping != null) {
                        columnName = colNameMapping.getOrDefault(column.getColumnName(), null);
                        // if column name has no corresponding field, then we just skip it
                        if (columnName == null) {
                            continue;
                        }
                    } else {
                        columnName = column.getColumnName();
                    }
                    projectedColNames.add(columnName);
                    String fullColumnName =
                            column.isNestedColumn()
                                    ? FileIndexCommon.toMapKey(
                                            columnName, column.getNestedColumnName())
                                    : column.getColumnName();
                    projectedColFullNames.add(fullColumnName);
                    projectedIndexTypes
                            .computeIfAbsent(fullColumnName, ignored -> new HashSet<>())
                            .addAll(entry.getValue().keySet());
                }

                schemaInfos.put(
                        schemaId,
                        new SchemaInfo(
                                fileSchema,
                                colNameMapping,
                                projectedColNames.stream()
                                        .mapToInt(fileSchema::getFieldIndex)
                                        .toArray(),
                                projectedColFullNames,
                                projectedIndexTypes));
                fileSchemaIds.add(schemaId);
            }

            return schemaInfos.get(schemaId);
        }

        private static Map<String, String> createIndexNameMapping(
                List<DataField> tableFields, List<DataField> dataFields) {
            Map<String, String> indexMapping = new HashMap<>();
            Map<Integer, String> fieldIdToIndex = new HashMap<>();
            for (DataField dataField : tableFields) {
                fieldIdToIndex.put(dataField.id(), dataField.name());
            }

            for (DataField tableField : dataFields) {
                String dataFieldIndex = fieldIdToIndex.getOrDefault(tableField.id(), null);
                if (dataFieldIndex != null) {
                    indexMapping.put(dataFieldIndex, tableField.name());
                }
            }

            return indexMapping;
        }
    }

    private static class SchemaInfo {

        private final RowType fileSchema;
        private final Map<String, String> colNameMapping;
        private final int[] projectedIndexCols;
        private final Set<String> projectedColFullNames;
        private final Map<String, Set<String>> projectedIndexTypes;

        private SchemaInfo(
                RowType fileSchema,
                Map<String, String> colNameMapping,
                int[] projectedIndexCols,
                Set<String> projectedColFullNames,
                Map<String, Set<String>> projectedIndexTypes) {
            this.fileSchema = fileSchema;
            this.colNameMapping = colNameMapping;
            this.projectedIndexCols = projectedIndexCols;
            this.projectedColFullNames = projectedColFullNames;
            this.projectedIndexTypes = projectedIndexTypes;
        }
    }
}
