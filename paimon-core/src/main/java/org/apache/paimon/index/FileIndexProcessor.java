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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
    private final long sizeInMeta;

    public FileIndexProcessor(FileStoreTable table) {
        this.table = table;
        this.fileIndexOptions = table.coreOptions().indexColumnsOptions();
        this.fileIO = table.fileIO();
        this.pathFactory = table.store().pathFactory();
        this.pathFactories = new DataFilePathFactories(pathFactory);
        this.schemaInfoCache =
                new SchemaCache(fileIndexOptions, new SchemaManager(fileIO, table.location()));
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
        Map<String, Map<String, byte[]>> maintainers;
        // load
        if (!indexFiles.isEmpty()) {
            String indexFile = indexFiles.get(0);
            try (FileIndexFormat.Reader indexReader =
                    FileIndexFormat.createReader(
                            fileIO.newInputStream(
                                    dataFilePathFactory.toAlignedPath(indexFile, dataFileMeta)),
                            schemaInfo.fileSchema)) {
                maintainers = indexReader.readAll();
            }
            newIndexPath =
                    createNewFileIndexFilePath(
                            dataFilePathFactory.toAlignedPath(indexFile, dataFileMeta));
        } else {
            maintainers = new HashMap<>();
            newIndexPath = dataFileToFileIndexPath(dataFilePathFactory.toPath(dataFileMeta));
        }

        // remove unnecessary
        for (Map.Entry<String, Map<String, byte[]>> entry : new HashSet<>(maintainers.entrySet())) {
            String name = entry.getKey();
            if (!schemaInfo.projectedColFullNames.contains(name)) {
                maintainers.remove(name);
            } else {
                Map<String, byte[]> indexTypeBytes = maintainers.get(name);
                for (String indexType : entry.getValue().keySet()) {
                    if (!indexTypeBytes.containsKey(indexType)) {
                        indexTypeBytes.remove(indexType);
                    }
                }
            }
        }

        // ignore close, do not close to write file, only collect serialized maintainers
        @SuppressWarnings("resource")
        DataFileIndexWriter dataFileIndexWriter =
                DataFileIndexWriter.create(
                        fileIO,
                        newIndexPath,
                        schemaInfo.fileSchema.project(schemaInfo.projectedIndexCols),
                        fileIndexOptions,
                        schemaInfo.colNameMapping);
        if (dataFileIndexWriter != null) {
            try (RecordReader<InternalRow> reader =
                    table.newReadBuilder()
                            .withProjection(schemaInfo.projectedIndexCols)
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
                                            .withDataFiles(Collections.singletonList(dataFileMeta))
                                            .rawConvertible(true)
                                            .build())) {
                reader.forEachRemaining(dataFileIndexWriter::write);
            }

            dataFileIndexWriter
                    .serializeMaintainers()
                    .forEach(
                            (key, value) ->
                                    maintainers
                                            .computeIfAbsent(key, k -> new HashMap<>())
                                            .putAll(value));
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (FileIndexFormat.Writer indexWriter = FileIndexFormat.createWriter(baos)) {
            if (!maintainers.isEmpty()) {
                indexWriter.writeColumnIndexes(maintainers);
            }
        }

        if (baos.size() > sizeInMeta) {
            try (OutputStream outputStream = fileIO.newOutputStream(newIndexPath, true)) {
                outputStream.write(baos.toByteArray());
            }
            extras.add(newIndexPath.getName());
            return dataFileMeta.copy(extras);
        } else if (baos.size() == 0) {
            return dataFileMeta.copy(extras);
        } else {
            return dataFileMeta.copy(baos.toByteArray());
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

                List<String> projectedColNames = new ArrayList<>();
                Set<String> projectedColFullNames = new HashSet<>();
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
                }

                schemaInfos.put(
                        schemaId,
                        new SchemaInfo(
                                fileSchema,
                                colNameMapping,
                                projectedColNames.stream()
                                        .mapToInt(fileSchema::getFieldIndex)
                                        .toArray(),
                                projectedColFullNames));
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

        private SchemaInfo(
                RowType fileSchema,
                Map<String, String> colNameMapping,
                int[] projectedIndexCols,
                Set<String> projectedColFullNames) {
            this.fileSchema = fileSchema;
            this.colNameMapping = colNameMapping;
            this.projectedIndexCols = projectedIndexCols;
            this.projectedColFullNames = projectedColFullNames;
        }
    }
}
